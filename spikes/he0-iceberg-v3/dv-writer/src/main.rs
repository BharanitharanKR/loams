// HE0 G2 (unmerged): write deletion vectors with iceberg-rust's DeletionVectorWriter
// (ostrium-labs/iceberg-rust, branch loams/dv-writer) for tables Spark wrote.
//
//   dv-writer-probe <catalog-uri> write <warehouse> <ns.table> <out.tsv>
//   dv-writer-probe <catalog-uri> read  <warehouse> <ns.table> [min-id filter]
//
// `write` deletes the rows at positions p with p % 7 == 3 from every data file, merging
// each file's existing deletion vector into the new one, and writes one Puffin file per
// partition. It commits nothing: it prints one TSV line per new deletion vector, which
// `commit.scala` commits with Iceberg Java's RowDelta (replacing the old vectors).
//
// `read` prints the fingerprint Spark and Trino print (`fingerprint.sql`).

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::delete_vector::DeleteVector;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, PartitionKey};
use iceberg::table::Table;
use iceberg::writer::base_writer::deletion_vector_writer::DeletionVectorWriterBuilder;
use iceberg::writer::base_writer::position_delete_writer::position_delete_schema;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
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

async fn write(table: &Table, out: &str) -> Res<()> {
    let builder = DeletionVectorWriterBuilder::new(
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata())?,
        DefaultFileNameGenerator::new(
            format!("he0-dvw-{}", std::process::id()),
            None,
            DataFileFormat::Puffin,
        ),
    );
    let schema = Arc::new(schema_to_arrow_schema(&position_delete_schema())?);
    // One writer, and so one Puffin file, per partition.
    let mut by_partition: BTreeMap<String, Vec<FileScanTask>> = BTreeMap::new();
    for task in plan(table).await? {
        by_partition.entry(format!("{:?}", task.partition())).or_default().push(task);
    }
    let mut tsv = std::fs::File::create(out)?;
    let (mut files, mut existing_total, mut new_total) = (0, 0u64, 0u64);
    for tasks in by_partition.values() {
        let first = &tasks[0];
        let key = match (first.partition_spec(), first.partition()) {
            (Some(spec), Some(data)) if !spec.is_unpartitioned() => Some(PartitionKey::new(
                spec.as_ref().clone(),
                table.metadata().current_schema().clone(),
                data.clone(),
            )),
            _ => None,
        };
        let mut writer = builder.build(key).await?;
        for task in tasks {
            let path = task.data_file_path();
            if let Some(existing) = existing_vector(table, task).await? {
                existing_total += existing.len();
                writer.merge_existing(path, existing)?;
            }
            let rows = task.record_count().ok_or("no record_count")? as i64;
            let positions: Vec<i64> = (0..rows).filter(|p| p % 7 == 3).collect();
            new_total += positions.len() as u64;
            writer
                .write(RecordBatch::try_new(schema.clone(), vec![
                    Arc::new(StringArray::from(vec![path; positions.len()])),
                    Arc::new(Int64Array::from(positions)),
                ])?)
                .await?;
        }
        for dv in writer.close().await? {
            files += 1;
            writeln!(
                tsv,
                "{}\t{}\t{}\t{}\t{}\t{}",
                dv.referenced_data_file().ok_or("no referenced_data_file")?,
                dv.file_path(),
                dv.file_size_in_bytes(),
                dv.record_count(),
                dv.content_offset().ok_or("no content_offset")?,
                dv.content_size_in_bytes().ok_or("no content_size")?,
            )?;
        }
    }
    println!(
        "  wrote {files} deletion vectors in {} Puffin files: {existing_total} existing + {new_total} new positions (overlaps counted once in the files)",
        by_partition.len()
    );
    Ok(())
}

async fn read(table: &Table, min_id: Option<i64>) -> Res<()> {
    let mut scan = table.scan().select(["id", "_row_id"]);
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
        let rid = b.column(1).as_any().downcast_ref::<Int64Array>().ok_or("_row_id")?;
        for i in 0..b.num_rows() {
            rows += 1;
            sum_id += i128::from(id.value(i));
            let r = if rid.is_null(i) { 0 } else { i128::from(rid.value(i)) };
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
        "write" => write(&table, &args[5]).await,
        "read" => read(&table, args.get(5).map(|s| s.parse()).transpose()?).await,
        other => Err(format!("unknown mode {other}").into()),
    }
}
