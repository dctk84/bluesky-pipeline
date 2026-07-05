"""Load Gold event volume prototype từ MinIO vào ClickHouse."""

from base64 import b64encode
from urllib.parse import quote
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, SparkSession

from bluesky_pipeline.spark_session import create_spark_session


GOLD_EVENT_VOLUME_PATH = "s3a://bluesky-lake/gold/gold_event_volume_by_type"
CLICKHOUSE_URL = "http://localhost:8123"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse"
CLICKHOUSE_TABLE = "bluesky.gold_event_volume_by_type"

def build_auth_header() -> str:
    """Tạo HTTP Basic Auth header cho ClickHouse local.

    Input lấy từ user/password cấu hình trong script.
    Output là giá trị header Authorization.
    """
    token = b64encode(
        f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}".encode("utf-8")
    ).decode("utf-8")
    return f"Basic {token}"

def execute_clickhouse(query: str, body: str | None = None) -> str:
    """Gửi query tới ClickHouse qua HTTP.

    Input chính là câu SQL và body optional cho INSERT.
    Output là response text từ ClickHouse.
    """
    url = f"{CLICKHOUSE_URL}/?query={quote(query)}"
    data = body.encode("utf-8") if body is not None else None

    request = Request(url, data=data, method="POST")
    request.add_header("Authorization", build_auth_header())

    with urlopen(request) as response:
        return response.read().decode("utf-8")


def read_gold_event_volume(spark: SparkSession) -> DataFrame:
    """Đọc Gold event volume prototype từ MinIO.

    Input chính là SparkSession đã cấu hình S3A.
    Output là DataFrame chứa event_type và event_count.
    """
    return spark.read.parquet(GOLD_EVENT_VOLUME_PATH)


def build_csv_payload(gold_df: DataFrame) -> str:
    """Chuyển Gold DataFrame nhỏ thành CSV payload để insert ClickHouse.

    Input chính là DataFrame Gold event volume.
    Output là chuỗi CSV không header.
    """
    rows = gold_df.select("event_type", "event_count").collect()

    return "".join(f"{row.event_type},{row.event_count}\n" for row in rows)


def load_gold_event_volume(gold_df: DataFrame) -> None:
    """Truncate và load lại Gold event volume vào ClickHouse.

    Input chính là DataFrame Gold event volume.
    Output là dữ liệu được ghi vào ClickHouse serving table.
    """
    # Rebuild bảng serving từ Gold prototype để đảm bảo kết quả idempotent ở local.
    execute_clickhouse(f"TRUNCATE TABLE {CLICKHOUSE_TABLE}")

    csv_payload = build_csv_payload(gold_df)
    execute_clickhouse(
        f"INSERT INTO {CLICKHOUSE_TABLE} (event_type,event_count) FORMAT CSV",
        body=csv_payload,
    )


def main() -> None:
    """Load Gold event volume vào ClickHouse và query kiểm chứng."""
    # Tạo SparkSession local có cấu hình đọc MinIO.
    spark = create_spark_session("bluesky-load-gold-event-volume-clickhouse")
    spark.sparkContext.setLogLevel("WARN")

    # Đọc Gold prototype, load vào ClickHouse và in kết quả query.
    gold_df = read_gold_event_volume(spark)
    load_gold_event_volume(gold_df)

    result = execute_clickhouse(
        f"SELECT event_type,event_count FROM {CLICKHOUSE_TABLE} ORDER BY event_type"
    )
    print(result)


if __name__ == "__main__":
    main()