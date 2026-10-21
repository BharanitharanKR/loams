// HE0 Task 11 spike (unmerged): one probe, compiled against iceberg-rust 0.10.1
// (reader-0.10) and against its main branch pinned by commit (reader-main).
//
//   reader <catalog-uri> read     <warehouse> <ns.table>...
//   reader <catalog-uri> lineage  <warehouse> <ns.table> [min-id filter]
//   reader <catalog-uri> cols     <warehouse> <ns.table> <col,col,...>
//   reader <catalog-uri> create   <warehouse> <ns> <name> <format-version>
//   reader <catalog-uri> append   <warehouse> <ns.table> <first-id> <rows>
//   reader <catalog-uri> upgrade  <warehouse> <ns.table>
//
// S3 settings come from HE0_S3_ENDPOINT, HE0_S3_KEY and HE0_S3_SECRET. Every
// failure is printed and the probe moves on: the point is the list of what
// fails, not stopping at the first one.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use futures::TryStreamExt;
use iceberg::spec::{DataFileFormat, FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder};
use iceberg_storage_opendal::OpenDalStorageFactory;
use parquet::file::properties::WriterProperties;

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

fn summarise(batches: &[RecordBatch]) -> String {
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut sum_id: i128 = 0;
    let mut has_id = false;
    let mut marks: HashMap<String, usize> = HashMap::new();
    for b in batches {
        if let Some(col) = b.column_by_name("id") {
            if let Some(ids) = col.as_any().downcast_ref::<Int64Array>() {
                has_id = true;
                sum_id += ids.iter().flatten().map(i128::from).sum::<i128>();
            }
        }
        if let Some(col) = b.column_by_name("v") {
            if let Some(vs) = col.as_any().downcast_ref::<StringArray>() {
                for v in vs.iter().flatten() {
                    if v.len() == 1 {
                        *marks.entry(v.to_string()).or_default() += 1;
                    }
                }
            }
        }
    }
    let mut marks: Vec<_> = marks.into_iter().collect();
    marks.sort();
    format!(
        "rows={rows}{} marks={marks:?}",
        if has_id { format!(" sum(id)={sum_id}") } else { String::new() }
    )
}

async fn read(cat: &impl Catalog, dotted: &str) -> Res<()> {
    let table = cat.load_table(&ident(dotted)?).await?;
    let m = table.metadata();
    let snap = m.current_snapshot();
    println!(
        "  metadata: format-version={:?} next-row-id={:?} snapshots={} current first-row-id={:?} added-rows={:?}",
        m.format_version(),
        m.next_row_id(),
        m.snapshots().count(),
        snap.and_then(|s| s.first_row_id()),
        snap.and_then(|s| s.added_rows_count()),
    );
    for f in m.current_schema().as_struct().fields() {
        println!("  field {} {} {} required={}", f.id, f.name, f.field_type, f.required);
    }
    let scan = table.scan().select_all().build()?;
    let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;
    let deletes: usize = tasks.iter().map(task_deletes).sum();
    println!("  plan: {} file tasks, {} delete files attached", tasks.len(), deletes);
    let batches: Vec<RecordBatch> = scan.to_arrow().await?.try_collect().await?;
    println!("  scan: {}", summarise(&batches));
    if let Some(b) = batches.first() {
        for (field, col) in b.schema().fields().iter().zip(b.columns()) {
            println!("  arrow {}: {:?} (nulls {})", field.name(), field.data_type(), col.null_count());
        }
        let shown = b.slice(0, b.num_rows().min(3));
        println!("{}", arrow_cast::pretty::pretty_format_batches(&[shown])?);
    }
    Ok(())
}

async fn lineage(cat: &impl Catalog, dotted: &str, min_id: Option<i64>) -> Res<()> {
    let table = cat.load_table(&ident(dotted)?).await?;
    let mut scan = table
        .scan()
        .select(["id", "v", "_row_id", "_last_updated_sequence_number"]);
    if let Some(min) = min_id {
        scan = scan.with_filter(
            iceberg::expr::Reference::new("id").greater_than_or_equal_to(iceberg::spec::Datum::long(min)),
        );
    }
    let scan = scan.build()?;
    let batches: Vec<RecordBatch> = scan.to_arrow().await?.try_collect().await?;
    let mut rows: Vec<(i64, String, Option<i64>, Option<i64>)> = Vec::new();
    if let Some(b) = batches.first() {
        for f in b.schema().fields() {
            println!("  arrow {}: {:?}", f.name(), f.data_type());
        }
    }
    let long = |a: &arrow_array::ArrayRef| -> Res<Int64Array> {
        let cast = arrow_cast::cast(a, &arrow_schema::DataType::Int64)?;
        Ok(cast.as_any().downcast_ref::<Int64Array>().ok_or("not Int64")?.clone())
    };
    for b in &batches {
        let id = long(b.column(0))?;
        let v = b.column(1).as_any().downcast_ref::<StringArray>().ok_or("v")?;
        let rid = long(b.column(2))?;
        let seq = long(b.column(3))?;
        let (id, rid, seq) = (&id, &rid, &seq);
        for i in 0..b.num_rows() {
            let get = |a: &Int64Array| (!a.is_null(i)).then(|| a.value(i));
            rows.push((id.value(i), v.value(i).to_string(), get(rid), get(seq)));
        }
    }
    rows.sort();
    for r in rows.iter().filter(|r| [0, 3, 13, 49, 50, 53, 99].contains(&r.0)) {
        println!("  lineage id={} v={} _row_id={:?} _last_updated_sequence_number={:?}", r.0, r.1, r.2, r.3);
    }
    println!(
        "  lineage rows={} null _row_id={} _row_id<>id={}",
        rows.len(),
        rows.iter().filter(|r| r.2.is_none()).count(),
        rows.iter().filter(|r| r.2 != Some(r.0)).count()
    );
    // A fingerprint to compare with Spark's: sum(_row_id), sum(_row_id * id),
    // sum(_last_updated_sequence_number).
    let fp = rows.iter().fold((0i128, 0i128, 0i128), |acc, r| {
        let rid = i128::from(r.2.unwrap_or(0));
        (acc.0 + rid, acc.1 + rid * i128::from(r.0), acc.2 + i128::from(r.3.unwrap_or(0)))
    });
    println!("  fingerprint sum_rid={} sum_rid_x_id={} sum_seq={}", fp.0, fp.1, fp.2);
    Ok(())
}

async fn cols(cat: &impl Catalog, dotted: &str, names: &str) -> Res<()> {
    let table = cat.load_table(&ident(dotted)?).await?;
    let scan = table.scan().select(names.split(',')).build()?;
    let batches: Vec<RecordBatch> = scan.to_arrow().await?.try_collect().await?;
    println!("  cols {names}: {}", summarise(&batches));
    Ok(())
}

async fn create(cat: &impl Catalog, ns: &str, name: &str, version: &str) -> Res<()> {
    let ns = NamespaceIdent::from_vec(ns.split('.').map(str::to_string).collect())?;
    if !cat.namespace_exists(&ns).await? {
        cat.create_namespace(&ns, HashMap::new()).await?;
    }
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;
    let asked = if version == "3" { FormatVersion::V3 } else { FormatVersion::V2 };
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .format_version(asked)
        .build();
    let made = cat.create_table(&ns, creation).await?;
    let loaded = cat.load_table(made.identifier()).await?;
    println!(
        "  asked {asked:?}: create returned {:?}, load returned {:?}",
        made.metadata().format_version(),
        loaded.metadata().format_version()
    );
    Ok(())
}

async fn append(cat: &impl Catalog, dotted: &str, first: i64, n: i64) -> Res<()> {
    let table = cat.load_table(&ident(dotted)?).await?;
    let before = table.metadata().next_row_id();
    let schema = table.metadata().current_schema().clone();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&schema)?);
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from_iter_values(first..first + n)),
            Arc::new(StringArray::from_iter_values((0..n).map(|_| "r"))),
        ],
    )?;
    let location = DefaultLocationGenerator::new(table.metadata())?;
    let names = DefaultFileNameGenerator::new(
        // The generator only counts from 0: the prefix must be unique per
        // writer, or a second writer overwrites the first one's file.
        format!("he0-{VERSION_TAG}-{first}"),
        None,
        DataFileFormat::Parquet,
    );
    let parquet = ParquetWriterBuilder::new(WriterProperties::default(), schema);
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet,
        table.file_io().clone(),
        location,
        names,
    );
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    writer.write(batch).await?;
    let files = writer.close().await?;
    for f in &files {
        println!("  wrote {} rows={} first_row_id={:?}", f.file_path(), f.record_count(), f.first_row_id());
    }
    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(files).apply(tx)?;
    let after = tx.commit(cat).await?;
    let snap = after.metadata().current_snapshot();
    println!(
        "  committed: next-row-id {before} -> {}, snapshot first-row-id={:?} added-rows={:?}",
        after.metadata().next_row_id(),
        snap.and_then(|s| s.first_row_id()),
        snap.and_then(|s| s.added_rows_count()),
    );
    Ok(())
}

async fn upgrade(cat: &impl Catalog, dotted: &str) -> Res<()> {
    let table = cat.load_table(&ident(dotted)?).await?;
    let before = table.metadata().format_version();
    let tx = Transaction::new(&table);
    let tx = tx
        .upgrade_table_version()
        .set_format_version(FormatVersion::V3)
        .apply(tx)?;
    let after = tx.commit(cat).await?;
    let reloaded = cat.load_table(after.identifier()).await?;
    println!(
        "  upgrade: {before:?} -> commit returned {:?}, load returned {:?}, next-row-id={:?}",
        after.metadata().format_version(),
        reloaded.metadata().format_version(),
        reloaded.metadata().next_row_id()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Res<()> {
    let args: Vec<String> = std::env::args().collect();
    let [_, uri, cmd, warehouse, rest @ ..] = args.as_slice() else {
        return Err("usage: reader <uri> <read|lineage|create|append|upgrade> <warehouse> ...".into());
    };
    let cat = catalog(uri, warehouse).await?;
    println!("iceberg-rust {}", VERSION);
    let outcome = match (cmd.as_str(), rest) {
        ("read", tables) => {
            for t in tables {
                println!("== read {t}");
                if let Err(err) = read(&cat, t).await {
                    println!("  FAILED: {err}");
                }
            }
            Ok(())
        }
        ("lineage", [t]) => lineage(&cat, t, None).await,
        ("lineage", [t, min]) => lineage(&cat, t, Some(min.parse()?)).await,
        ("cols", [t, names]) => cols(&cat, t, names).await,
        ("create", [ns, name, v]) => create(&cat, ns, name, v).await,
        ("append", [t, first, n]) => append(&cat, t, first.parse()?, n.parse()?).await,
        ("upgrade", [t]) => upgrade(&cat, t).await,
        _ => Err("unknown command or wrong arguments".into()),
    };
    if let Err(err) = outcome {
        println!("  FAILED: {err}");
    }
    Ok(())
}
