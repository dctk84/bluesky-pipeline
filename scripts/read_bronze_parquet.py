"""Đọc dữ liệu Bronze Parquet local để kiểm chứng Spark đã ghi được dữ liệu usable."""

from pyspark.sql import SparkSession


BRONZE_INPUT_PATH = "s3a://bluesky-lake/bronze/bluesky_raw_events"


def create_spark_session() -> SparkSession:
    """Tạo SparkSession local để đọc dữ liệu Parquet Bronze từ MinIO."""
    return (
        SparkSession.builder
        .appName("bluesky-read-bronze-parquet")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main() -> None:
    """Đọc Bronze Parquet, in schema và một số dòng mẫu để kiểm chứng dữ liệu."""
    # Bước 1: Tạo SparkSession.
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Bước 2: Đọc dữ liệu Parquet đã được Spark streaming ghi ra Bronze local.
    bronze_df = spark.read.parquet(BRONZE_INPUT_PATH)

    # Bước 3: In schema để kiểm tra các cột envelope đã được lưu đúng.
    bronze_df.printSchema()

    # Bước 4: In một số dòng mẫu để kiểm chứng dữ liệu đọc lại được.
    bronze_df.show(10, truncate=False)

    # Bước 5: In số lượng record hiện có trong Bronze local.
    print(f"bronze_count: {bronze_df.count()}")


if __name__ == "__main__":
    main()