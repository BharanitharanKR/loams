// HE0 G3 (unmerged): commit deletes with iceberg-rust's RowDelta (ostrium-labs/iceberg-rust,
// branch loams/row-delta) to tables Spark wrote.
//
//   row-delta-probe <catalog-uri> commit <warehouse> <ns.table>
//   row-delta-probe <catalog-uri> read   <warehouse> <ns.table> [min-id filter]
//
// `commit` deletes the rows at positions p with p % 7 == 3 from every data file in one RowDelta
// commit. On a v3 table it writes deletion vectors (DeletionVectorWriter, one Puffin file per
// partition), merging each file's existing vector and removing it in the same commit. On a v2
// table it adds Parquet position delete files (PositionDeleteFileWriter, one per partition) and
// leaves the existing ones alone.
//
// `read` prints the fingerprint Spark, Trino and DuckDB print.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::delete_vector::DeleteVector;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFile, DataFileFormat, FormatVersion, PartitionKey};
use iceberg::table::Table;
use iceberg::writer::base_writer::deletion_vector_writer::DeletionVectorWriterBuilder;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::position_delete_writer::{
    PositionDeleteFileWriterBuilder, position_delete_schema,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use parquet::file::properties::WriterProperties;
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

async fn catalog(uri: &str, warehouse: &str) -> Res<impl Catalog> {
    let props = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), uri.to_string()),
        (REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.to_string()),
        ("s3.endpoint".to_string(), env("HE0_S3_ENDPOINT", "http://127.0.0.1:19000")),
        ("s3.access-key-id".to_string(), env("HE0_S3_KEY", "spikeadmin")),
        ("s3.secret-access-key".to_string(), env("HE0_S3_SECRET", "spikeadmin-secret")),
        ("s3.region".to_string(), "us-east-1".to_string()),
        ("s3.path-style-access".to_string(), "true".to_string()),
    ]);
    Ok(RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("spike", props)
        .await?)
}

fn ident(dotted: &str) -> Res<TableIdent> {
    let mut parts: Vec<String> = dotted.split('.').map(str::to_string).collect();
    let name = parts.pop().ok_or("empty table name")?;
    Ok(TableIdent::new(NamespaceIdent::from_vec(parts)?, name))
}

async fn plan(table: &Table) -> Res<Vec<FileScanTask>> {
    Ok(table.scan().build()?.plan_files().await?.try_collect().await?)
}

/// The positions the task's existing deletion vector deletes, if it has one.
async fn existing_vector(table: &Table, task: &FileScanTask) -> Res<Option<DeleteVector>> {
    match task.deletes() {
        [] => Ok(None),
        [dv] if dv.file_format() == DataFileFormat::Puffin => {
            let bytes = table.file_io().new_input(dv.file_path())?.read().await?;
            let start = dv.content_offset().ok_or("no content_offset")? as usize;
            let end = start + dv.content_size_in_bytes().ok_or("no content_size")? as usize;
            Ok(Some(DeleteVector::deserialize(&bytes[start..end])?))
        }
        other => Err(format!("unsupported deletes for {}: {other:?}", task.data_file_path()).into()),
    }
}

/// The live manifest entry of the deletion vector at `path` and `offset`.
async fn live_dv(table: &Table, path: &str, offset: Option<u64>) -> Res<DataFile> {
    let snapshot = table.metadata().current_snapshot().ok_or("no snapshot")?;
    let list = table.manifest_list_reader(snapshot).load().await?;
    for manifest_file in list.entries() {
        let manifest = table.manifest_reader().read(manifest_file).await?;
        for entry in manifest.entries() {
            let file = entry.data_file();
            if entry.is_alive()
                && file.file_path() == path
                && file.content_offset().map(|o| o as u64) == offset
            {
                return Ok(file.clone());
            }
        }
    }
    Err(format!("no live deletion vector {path} at {offset:?}").into())
}

fn delete_batch(task: &FileScanTask) -> Res<(RecordBatch, u64)> {
    let schema = Arc::new(schema_to_arrow_schema(&position_delete_schema())?);
    let rows = task.record_count().ok_or("no record_count")? as i64;
    let positions: Vec<i64> = (0..rows).filter(|p| p % 7 == 3).collect();
    let n = positions.len() as u64;
    Ok((
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec![task.data_file_path(); positions.len()])),
            Arc::new(Int64Array::from(positions)),
        ])?,
        n,
    ))
}

async fn commit(cat: &impl Catalog, table: &Table) -> Res<()> {
    let v3 = table.metadata().format_version() == FormatVersion::V3;
    let tag = format!("he0-rd-{}", std::process::id());
    let location = DefaultLocationGenerator::new(table.metadata())?;
    // One writer, and so one delete file, per partition.
    let mut by_partition: BTreeMap<String, Vec<FileScanTask>> = BTreeMap::new();
    for task in plan(table).await? {
        by_partition.entry(format!("{:?}", task.partition())).or_default().push(task);
    }
    let (mut added, mut removed) = (Vec::new(), Vec::new());
    let (mut existing_total, mut new_total) = (0u64, 0u64);
    for mut tasks in by_partition.into_values() {
        tasks.sort_by(|a, b| a.data_file_path().cmp(b.data_file_path()));
        let first = &tasks[0];
        let key = match (first.partition_spec(), first.partition()) {
            (Some(spec), Some(data)) if !spec.is_unpartitioned() => Some(PartitionKey::new(
                spec.as_ref().clone(),
                table.metadata().current_schema().clone(),
                data.clone(),
            )),
            _ => None,
        };
        if v3 {
            let mut writer = DeletionVectorWriterBuilder::new(
                table.file_io().clone(),
                location.clone(),
                DefaultFileNameGenerator::new(tag.clone(), None, DataFileFormat::Puffin),
            )
            .build(key)
            .await?;
            for task in &tasks {
                if let Some(existing) = existing_vector(table, task).await? {
                    existing_total += existing.len();
                    writer.merge_existing(task.data_file_path(), existing)?;
                    let old = &task.deletes()[0];
                    removed.push(live_dv(table, old.file_path(), old.content_offset()).await?);
                }
                let (batch, n) = delete_batch(task)?;
                new_total += n;
                writer.write(batch).await?;
            }
            added.extend(writer.close().await?);
        } else {
            let rolling = RollingFileWriterBuilder::new_with_default_file_size(
                ParquetWriterBuilder::new(WriterProperties::default(), position_delete_schema()),
                table.file_io().clone(),
                location.clone(),
                DefaultFileNameGenerator::new(tag.clone(), None, DataFileFormat::Parquet),
            );
            let mut writer = PositionDeleteFileWriterBuilder::new(rolling).build(key).await?;
            for task in &tasks {
                existing_total += task.deletes().iter().filter_map(|d| d.record_count()).sum::<u64>();
                let (batch, n) = delete_batch(task)?;
                new_total += n;
                writer.write(batch).await?;
            }
            added.extend(writer.close().await?);
        }
    }
    let (n_added, n_removed) = (added.len(), removed.len());
    let tx = Transaction::new(table);
    let tx = tx
        .row_delta()
        .add_delete_files(added)
        .remove_delete_files(removed)
        .apply(tx)?;
    let committed = tx.commit(cat).await?;
    let snap = committed.metadata().current_snapshot().ok_or("no snapshot")?;
    println!(
        "  committed {} snapshot {} ({:?}): {n_added} delete files added, {n_removed} replaced; {existing_total} existing + {new_total} new positions",
        if v3 { "v3 deletion vectors" } else { "v2 position deletes" },
        snap.snapshot_id(),
        snap.summary().operation,
    );
    Ok(())
}

async fn read(table: &Table, min_id: Option<i64>) -> Res<()> {
    // A v2 table has no row lineage: its fingerprint has zero lineage sums.
    let v3 = table.metadata().format_version() == FormatVersion::V3;
    let columns: &[&str] = if v3 { &["id", "_row_id"] } else { &["id"] };
    let mut scan = table.scan().select(columns.iter().copied());
    if let Some(min) = min_id {
        scan = scan.with_filter(
            iceberg::expr::Reference::new("id")
                .greater_than_or_equal_to(iceberg::spec::Datum::long(min)),
        );
    }
    let batches: Vec<RecordBatch> = scan.build()?.to_arrow().await?.try_collect().await?;
    let (mut rows, mut sum_id, mut sum_rid, mut sum_rid_x_id) = (0usize, 0i128, 0i128, 0i128);
    for b in &batches {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().ok_or("id")?;
        let rid = if v3 {
            Some(b.column(1).as_any().downcast_ref::<Int64Array>().ok_or("_row_id")?)
        } else {
            None
        };
        for i in 0..b.num_rows() {
            rows += 1;
            sum_id += i128::from(id.value(i));
            let r = match rid {
                Some(rid) if !rid.is_null(i) => i128::from(rid.value(i)),
                _ => 0,
            };
            sum_rid += r;
            sum_rid_x_id += r * i128::from(id.value(i));
        }
    }
    println!("fp\t{rows}\t{sum_id}\t{sum_rid}\t{sum_rid_x_id}");
    Ok(())
}

#[tokio::main]
async fn main() -> Res<()> {
    let args: Vec<String> = std::env::args().collect();
    let (uri, mode, warehouse, table) = (&args[1], &args[2], &args[3], &args[4]);
    let cat = catalog(uri, warehouse).await?;
    let table = cat.load_table(&ident(table)?).await?;
    match mode.as_str() {
        "commit" => commit(&cat, &table).await,
        "read" => read(&table, args.get(5).map(|s| s.parse()).transpose()?).await,
        other => Err(format!("unknown mode {other}").into()),
    }
}
