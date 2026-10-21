// HE0 G2 (unmerged): commit the deletion vectors iceberg-rust wrote (dvs-<table>.tsv) with
// Iceberg Java's RowDelta, replacing each data file's existing deletion vector.
// Run by run.sh through spark-shell, with spark.he0.table = <ns>.<table>.
import scala.jdk.CollectionConverters._
import org.apache.iceberg.{FileFormat, FileMetadata}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.SparkCatalog

val name = spark.conf.get("spark.he0.table")
val Array(ns, tbl) = name.split('.')
val catalog = spark.sessionState.catalogManager.catalog("spark").asInstanceOf[SparkCatalog]
val table = catalog.icebergCatalog.loadTable(TableIdentifier.of(ns, tbl))
val tasks = table.newScan().planFiles().asScala.map(t => t.file().location() -> t).toMap
// Validate from the snapshot the vectors were merged against, as Spark does; without it
// RowDelta treats every earlier deletion vector as concurrently added.
val delta = table.newRowDelta().validateFromSnapshot(table.currentSnapshot().snapshotId())
var added, removed = 0
for (line <- scala.io.Source.fromFile(s"/work/dvs-$tbl.tsv").getLines()) {
  val Array(dataFile, path, size, cardinality, offset, length) = line.split('\t')
  val task = tasks(dataFile)
  delta.addDeletes(FileMetadata.deleteFileBuilder(task.spec())
    .ofPositionDeletes()
    .withFormat(FileFormat.PUFFIN)
    .withPath(path)
    .withFileSizeInBytes(size.toLong)
    .withPartition(task.file().partition())
    .withRecordCount(cardinality.toLong)
    .withReferencedDataFile(dataFile)
    .withContentOffset(offset.toLong)
    .withContentSizeInBytes(length.toLong)
    .build())
  added += 1
  for (old <- task.deletes().asScala) { delta.removeDeletes(old); removed += 1 }
}
delta.commit()
println(s"committed $name: $added deletion vectors added, $removed replaced")
System.exit(0)
