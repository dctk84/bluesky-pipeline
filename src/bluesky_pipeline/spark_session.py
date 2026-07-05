import os

from pyspark.sql import SparkSession


HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"

S3A_ENDPOINT = os.getenv("S3A_ENDPOINT", "http://localhost:9000")
S3A_ACCESS_KEY = os.getenv("S3A_ACCESS_KEY", "minioadmin")
S3A_SECRET_KEY = os.getenv("S3A_SECRET_KEY", "minioadmin")


def create_spark_session(app_name: str, extra_packages: list[str] | None = None) -> SparkSession:
    """Tạo SparkSession local đã cấu hình S3A để đọc/ghi MinIO.

    Input chính là tên app Spark và danh sách package bổ sung.
    Output là SparkSession có thể truy cập MinIO qua scheme s3a://.
    """
    packages = [HADOOP_AWS_PACKAGE]

    if extra_packages:
        packages.extend(extra_packages)

    # Cấu hình Spark local và các package cần tải qua Maven.
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", ",".join(packages))
    )

    # Cấu hình S3A để Spark đọc/ghi dữ liệu trên MinIO local.
    return (
        builder
        .config("spark.hadoop.fs.s3a.endpoint", S3A_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3A_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3A_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )