"""Đọc dữ liệu Bronze Parquet local để kiểm chứng Spark đã ghi được dữ liệu usable."""

from pyspark.sql import SparkSession


BRONZE_INPUT_PATH = "data/bronze/bluesky_raw_events"


def create_spark_session() -> SparkSession:
    """Tạo SparkSession local để đọc dữ liệu Parquet đã ghi ở tầng Bronze."""
    return (
        SparkSession.builder
        .appName("bluesky-read-bronze-parquet")
        .master("local[*]")
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