"""Stream Bronze commit events sang Silver Iceberg."""

from __future__ import annotations

from pyspark.errors import AnalysisException

from bluesky_pipeline.config.iceberg import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_SILVER_NAMESPACE,
    ICEBERG_SILVER_STREAM_CHECKPOINT_LOCATION,
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)
from bluesky_pipeline.schemas.bronze_tables import BRONZE_COMMIT_EVENTS_PATH
from bluesky_pipeline.transforms.silver_transformations import (
    SILVER_TRANSFORMATIONS,
    read_bronze_commit_events_stream,
)


def ensure_silver_namespace(spark) -> None:
    """Tạo namespace Iceberg cho Silver v1 nếu chưa tồn tại."""
    # Namespace tương đương database logic cho các bảng Silver v1.
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_SILVER_NAMESPACE}"
    )


def iceberg_table_exists(spark, iceberg_table: str) -> bool:
    """Kiểm tra một bảng Iceberg đã tồn tại trong catalog hay chưa."""
    try:
        spark.sql(f"SELECT 1 FROM {iceberg_table} LIMIT 1").collect()
        return True
    except AnalysisException:
        return False


def ensure_bronze_commit_path_exists(spark) -> None:
    """Tạo Bronze commit path rỗng nếu cleanup vừa xóa path này."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
        BRONZE_COMMIT_EVENTS_PATH
    )
    filesystem = hadoop_path.getFileSystem(hadoop_conf)

    if not filesystem.exists(hadoop_path):
        filesystem.mkdirs(hadoop_path)


def write_silver_table(batch_df, table_name: str) -> int:
    """Transform một micro-batch Bronze và append vào một bảng Silver."""
    spark = batch_df.sparkSession
    iceberg_table = ICEBERG_SILVER_TABLES[table_name]
    silver_df = SILVER_TRANSFORMATIONS[table_name](batch_df)

    # Tạo table bằng schema của micro-batch, kể cả khi batch chưa có row loại đó.
    if not iceberg_table_exists(spark, iceberg_table):
        silver_df.limit(0).writeTo(iceberg_table).using("iceberg").create()

    row_count = silver_df.count()
    if row_count > 0:
        silver_df.writeTo(iceberg_table).append()

    return row_count


def write_silver_batch(batch_df, batch_id: int) -> None:
    """Ghi một micro-batch Bronze sang toàn bộ Silver Iceberg tables."""
    # Cache batch vì cùng một Bronze micro-batch được dùng để build 4 bảng Silver.
    cached_batch_df = batch_df.cache()
    input_rows = cached_batch_df.count()

    if input_rows == 0:
        print(f"silver_stream_batch_id={batch_id} input_rows=0 skipped", flush=True)
        cached_batch_df.unpersist()
        return

    written_counts = {
        table_name: write_silver_table(cached_batch_df, table_name)
        for table_name in SILVER_TRANSFORMATIONS
    }

    cached_batch_df.unpersist()

    print(
        f"silver_stream_batch_id={batch_id} input_rows={input_rows} "
        + " ".join(
            f"{table_name}_rows={row_count}"
            for table_name, row_count in written_counts.items()
        ),
        flush=True,
    )


def main() -> None:
    """Chạy streaming Bronze -> Silver Iceberg cho live pipeline."""
    spark = create_iceberg_spark_session("bluesky-stream-silver-from-bronze")
    spark.sparkContext.setLogLevel("WARN")

    ensure_silver_namespace(spark)
    ensure_bronze_commit_path_exists(spark)
    bronze_stream_df = read_bronze_commit_events_stream(spark)

    query = (
        bronze_stream_df.writeStream
        .foreachBatch(write_silver_batch)
        .option("checkpointLocation", ICEBERG_SILVER_STREAM_CHECKPOINT_LOCATION)
        .trigger(processingTime="60 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
