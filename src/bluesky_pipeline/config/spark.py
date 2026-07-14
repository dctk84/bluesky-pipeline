import os

from pyspark.sql import SparkSession


HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"

S3A_ENDPOINT = os.getenv("S3A_ENDPOINT", "http://localhost:9000")
S3A_ACCESS_KEY = os.getenv("S3A_ACCESS_KEY", "minioadmin")
S3A_SECRET_KEY = os.getenv("S3A_SECRET_KEY", "minioadmin")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[2]")
SPARK_SQL_SHUFFLE_PARTITIONS = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
SPARK_DEFAULT_PARALLELISM = os.getenv("SPARK_DEFAULT_PARALLELISM", "8")
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1g")
SPARK_DRIVER_MAX_RESULT_SIZE = os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "512m")
SPARK_SQL_SESSION_TIMEZONE = os.getenv("SPARK_SQL_SESSION_TIMEZONE", "UTC")


def create_spark_session(
    app_name: str,
    extra_packages: list[str] | None = None,
    extra_configs: dict[str, str] | None = None,
) -> SparkSession:
    """Tạo SparkSession local đã cấu hình S3A để đọc/ghi MinIO.

    Input chính là tên app Spark và danh sách package bổ sung.
    Output là SparkSession có thể truy cập MinIO qua scheme s3a://.
    """
    packages = [HADOOP_AWS_PACKAGE]

    if extra_packages:
        packages.extend(extra_packages)

    # Local WSL dễ quá tải nếu nhiều Spark app cùng dùng local[*].
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    # Cấu hình Spark local và các package cần tải qua Maven.
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", SPARK_SQL_SESSION_TIMEZONE)
        .config("spark.default.parallelism", SPARK_DEFAULT_PARALLELISM)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.driver.maxResultSize", SPARK_DRIVER_MAX_RESULT_SIZE)
    )

    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    # Cấu hình S3A để Spark đọc/ghi dữ liệu trên MinIO local.
    return (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", S3A_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3A_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3A_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
