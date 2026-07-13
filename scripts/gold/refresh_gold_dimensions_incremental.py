"""Discovery incremental scope cho Gold dimension tables từ Silver Iceberg."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count as spark_count,
    countDistinct,
    lit,
    to_timestamp,
)

from bluesky_pipeline.gold_transformations import (
    build_gold_dim_actors,
    build_gold_dim_posts,
)
from bluesky_pipeline.iceberg_config import (
    ICEBERG_CATALOG_NAME,
    ICEBERG_GOLD_NAMESPACE,
    ICEBERG_GOLD_TABLES,
    ICEBERG_SILVER_TABLES,
    create_iceberg_spark_session,
)
from bluesky_pipeline.incremental_refresh import (
    RefreshWindow,
    build_refresh_window,
    read_last_successful_run_at,
    utc_now,
    write_last_successful_run_at,
)
from scripts.gold.check_gold_dimensions_incremental import check_gold_dimension_keys


DEFAULT_STATE_PATH = Path("data/state/gold_dimensions_incremental_refresh.json")
SILVER_DIM_SOURCE_TABLES = [
    "silver_posts",
    "silver_engagements",
    "silver_follows",
    "silver_deleted_records",
]
GOLD_DIM_SPECS = [
    (
        "gold_dim_posts",
        "post_uri",
        [
            "post_uri",
            "author_did",
            "post_cid",
            "post_text",
            "post_created_at",
            "text_length",
            "is_reply",
            "reply_root_uri",
            "reply_parent_uri",
            "is_deleted",
            "deleted_at",
            "first_seen_at",
            "last_seen_at",
            "ingest_date",
            "ingest_hour",
        ],
    ),
    (
        "gold_dim_actors",
        "actor_did",
        [
            "actor_did",
            "first_seen_at",
            "last_seen_at",
            "first_ingest_date",
            "last_ingest_date",
            "source_event_count",
        ],
    ),
]


def _format_timestamp_for_spark(value: datetime) -> str:
    """Format datetime UTC thành chuỗi ISO có timezone để Spark parse ổn định.

    Input là datetime timezone-aware.
    Output là chuỗi ISO-8601 kết thúc bằng `Z`, cùng semantics với `received_at`.
    """
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def filter_by_received_at(
    table_df: DataFrame,
    refresh_window: RefreshWindow,
) -> DataFrame:
    """Lọc Silver rows theo refresh window dựa trên `received_at`.

    Input là Silver DataFrame và refresh window.
    Output là DataFrame chỉ gồm rows nằm trong phạm vi cần xử lý.
    """
    received_at_ts = to_timestamp(col("received_at"))
    refresh_to_ts = to_timestamp(
        lit(_format_timestamp_for_spark(refresh_window.refresh_to))
    )
    upper_bounded_df = table_df.filter(received_at_ts < refresh_to_ts)

    # Initial refresh dùng datetime.min làm marker logic. Không đưa marker này vào
    # filter Spark vì một số engine không parse ổn định timestamp năm 0001.
    if refresh_window.refresh_from == datetime.min.replace(tzinfo=timezone.utc):
        return upper_bounded_df

    refresh_from_ts = to_timestamp(
        lit(_format_timestamp_for_spark(refresh_window.refresh_from))
    )

    return upper_bounded_df.filter(received_at_ts >= refresh_from_ts)


def parse_args() -> argparse.Namespace:
    """Đọc CLI flags cho Gold dimension incremental refresh.

    Output là các option điều khiển cách tính refresh window.
    """
    parser = argparse.ArgumentParser(
        description="Discover affected Gold dimension keys from Silver Iceberg."
    )
    parser.add_argument(
        "--ignore-state",
        action="store_true",
        help="Bỏ qua local state và chạy như initial refresh.",
    )
    return parser.parse_args()


def read_incremental_silver_tables(
    spark,
    refresh_window: RefreshWindow,
) -> dict[str, DataFrame]:
    """Đọc các Silver tables trong refresh window.

    Input là SparkSession và refresh window.
    Output là mapping tên bảng Silver sang DataFrame đã cache.
    """
    incremental_silver_tables = {}

    for table_name in SILVER_DIM_SOURCE_TABLES:
        table_df = spark.table(ICEBERG_SILVER_TABLES[table_name])
        incremental_df = filter_by_received_at(table_df, refresh_window)
        incremental_silver_tables[table_name] = incremental_df.cache()
        print(
            f"{table_name}_incremental_rows: "
            f"{incremental_silver_tables[table_name].count()}"
        )

    return incremental_silver_tables


def build_affected_post_keys(
    incremental_silver_tables: dict[str, DataFrame],
) -> DataFrame:
    """Xác định các post_uri ảnh hưởng trực tiếp tới `gold_dim_posts`.

    Input là các Silver DataFrame incremental.
    Output là DataFrame một cột `post_uri` đã distinct.
    """
    posts_df = incremental_silver_tables["silver_posts"].select("post_uri")
    deleted_posts_df = (
        incremental_silver_tables["silver_deleted_records"]
        .filter(col("collection") == "app.bsky.feed.post")
        .select(col("record_uri").alias("post_uri"))
    )

    return (
        posts_df.unionByName(deleted_posts_df)
        .filter(col("post_uri").isNotNull())
        .dropDuplicates(["post_uri"])
    )


def build_affected_downstream_post_keys(
    incremental_silver_tables: dict[str, DataFrame],
) -> DataFrame:
    """Xác định các post_uri ảnh hưởng downstream marts.

    Input là các Silver DataFrame incremental.
    Output là DataFrame một cột `post_uri` đã distinct.
    """
    dim_post_keys_df = build_affected_post_keys(incremental_silver_tables)
    engaged_posts_df = incremental_silver_tables["silver_engagements"].select(
        col("subject_uri").alias("post_uri")
    )

    # Engagement mới không đổi bản thân gold_dim_posts, nhưng có thể làm thay đổi
    # các mart downstream như post performance nên giữ thành scope riêng.
    return (
        dim_post_keys_df.unionByName(engaged_posts_df)
        .filter(col("post_uri").isNotNull())
        .dropDuplicates(["post_uri"])
    )


def build_affected_actor_keys(
    incremental_silver_tables: dict[str, DataFrame],
) -> DataFrame:
    """Xác định các actor_did bị ảnh hưởng trong refresh window.

    Input là các Silver DataFrame incremental.
    Output là DataFrame một cột `actor_did` đã distinct.
    """
    post_authors_df = incremental_silver_tables["silver_posts"].select(
        col("author_did").alias("actor_did")
    )
    engagement_actors_df = incremental_silver_tables["silver_engagements"].select(
        "actor_did"
    )
    follow_actors_df = incremental_silver_tables["silver_follows"].select("actor_did")
    follow_targets_df = incremental_silver_tables["silver_follows"].select(
        col("target_actor_did").alias("actor_did")
    )
    delete_actors_df = incremental_silver_tables["silver_deleted_records"].select(
        col("repository_did").alias("actor_did")
    )

    return (
        post_authors_df.unionByName(engagement_actors_df)
        .unionByName(follow_actors_df)
        .unionByName(follow_targets_df)
        .unionByName(delete_actors_df)
        .filter(col("actor_did").isNotNull())
        .dropDuplicates(["actor_did"])
    )


def print_key_samples(table_name: str, key_df: DataFrame, key_column: str) -> None:
    """In count và sample key để kiểm chứng affected scope.

    Input là tên logical scope, DataFrame key và tên cột key.
    Output là count và tối đa 10 key sample.
    """
    key_count = key_df.count()
    print(f"{table_name}_count: {key_count}")

    print(f"{table_name}_sample:")
    key_df.orderBy(key_column).show(10, truncate=False)


def read_full_silver_tables(spark) -> dict[str, DataFrame]:
    """Đọc toàn bộ Silver tables để recompute dimension state cho affected keys.

    Input là SparkSession đọc được Iceberg catalog.
    Output là mapping tên bảng Silver sang DataFrame đã cache.
    """
    return {
        table_name: spark.table(iceberg_table).cache()
        for table_name, iceberg_table in ICEBERG_SILVER_TABLES.items()
    }


def ensure_gold_namespace(spark: SparkSession) -> None:
    """Tạo Gold namespace nếu chưa tồn tại.

    Input chính là SparkSession có cấu hình Iceberg catalog.
    Output là namespace Gold sẵn sàng cho dimension tables.
    """
    spark.sql(
        f"CREATE NAMESPACE IF NOT EXISTS "
        f"{ICEBERG_CATALOG_NAME}.{ICEBERG_GOLD_NAMESPACE}"
    )


def iceberg_table_exists(spark: SparkSession, iceberg_table: str) -> bool:
    """Kiểm tra một bảng Iceberg đã tồn tại trong catalog hay chưa.

    Input là SparkSession và tên bảng Iceberg đầy đủ.
    Output là True nếu bảng đọc được qua catalog.
    """
    try:
        spark.table(iceberg_table).limit(0).count()
        return True
    except AnalysisException:
        return False


def ensure_gold_dimension_table(
    spark: SparkSession,
    table_name: str,
    source_df: DataFrame,
) -> str:
    """Tạo Gold dimension table nếu bảng chưa tồn tại và bật Iceberg v2.

    Input là tên logical table và DataFrame nguồn có schema cần ghi.
    Output là tên bảng Iceberg đầy đủ trong catalog.
    """
    iceberg_table = ICEBERG_GOLD_TABLES[table_name]

    if not iceberg_table_exists(spark, iceberg_table):
        (
            source_df.limit(0)
            .writeTo(iceberg_table)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .create()
        )
        print(f"created_gold_dimension_table: {iceberg_table}")
    else:
        # MERGE INTO là row-level operation nên target nên dùng Iceberg format v2.
        spark.sql(
            f"ALTER TABLE {iceberg_table} "
            "SET TBLPROPERTIES ('format-version' = '2')"
        )
        print(f"ensured_iceberg_format_version_2: {iceberg_table}")

    return iceberg_table


def print_dimension_key_metrics(
    table_name: str,
    table_df: DataFrame,
    key_column: str,
    metric_scope: str = "affected",
) -> None:
    """In row count và key uniqueness cho affected dimension rows.

    Input là tên dimension, DataFrame cần kiểm tra, cột business key và scope
    metric. Output là metric giúp kiểm tra trước/sau khi merge dimension.
    """
    metrics = table_df.agg(
        spark_count("*").alias("row_count"),
        spark_count(key_column).alias("non_null_key_count"),
        countDistinct(key_column).alias("distinct_key_count"),
    ).collect()[0]
    row_count = int(metrics["row_count"] or 0)
    non_null_key_count = int(metrics["non_null_key_count"] or 0)
    distinct_key_count = int(metrics["distinct_key_count"] or 0)
    status = (
        "OK"
        if row_count == non_null_key_count == distinct_key_count
        else "MISMATCH"
    )

    print(f"{table_name}_{metric_scope}_rows: {row_count}")
    print(f"{table_name}_{metric_scope}_non_null_keys: {non_null_key_count}")
    print(f"{table_name}_{metric_scope}_distinct_keys: {distinct_key_count}")
    print(f"{table_name}_{metric_scope}_key_status: {status}")

    if status != "OK":
        raise RuntimeError(f"{table_name} {metric_scope} key check failed")


def merge_dimension_rows(
    spark: SparkSession,
    table_name: str,
    source_df: DataFrame,
    key_column: str,
    columns: list[str],
) -> None:
    """Merge affected dimension rows vào Gold Iceberg table.

    Input là source affected rows, key column và danh sách cột của dimension.
    Output là Gold dimension table được update/insert bằng Iceberg MERGE INTO.
    """
    print_dimension_key_metrics(table_name, source_df, key_column)
    iceberg_table = ensure_gold_dimension_table(spark, table_name, source_df)
    source_view = f"{table_name}_merge_source"
    source_df.select(*columns).createOrReplaceTempView(source_view)

    update_assignments = ",\n            ".join(
        f"{column} = source.{column}" for column in columns if column != key_column
    )
    insert_columns = ", ".join(columns)
    insert_values = ", ".join(f"source.{column}" for column in columns)

    spark.sql(
        f"""
        MERGE INTO {iceberg_table} AS target
        USING {source_view} AS source
        ON target.{key_column} = source.{key_column}
        WHEN MATCHED THEN UPDATE SET
            {update_assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
        """
    )
    print(f"{table_name}_merge_completed: true")

    merged_df = spark.table(iceberg_table)
    print_dimension_key_metrics(
        table_name,
        merged_df,
        key_column,
        metric_scope="iceberg_table",
    )


def build_affected_dimension_rows(
    full_silver_tables: dict[str, DataFrame],
    affected_post_keys_df: DataFrame,
    affected_actor_keys_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Build affected Gold dimension rows từ Silver source of truth.

    Input là full Silver tables và affected business keys trong refresh window.
    Output là affected `gold_dim_posts` và `gold_dim_actors` DataFrames.
    """
    gold_dim_posts_df = build_gold_dim_posts(full_silver_tables)
    gold_dim_actors_df = build_gold_dim_actors(full_silver_tables)

    affected_gold_dim_posts_df = gold_dim_posts_df.join(
        affected_post_keys_df,
        "post_uri",
        "inner",
    )
    affected_gold_dim_actors_df = gold_dim_actors_df.join(
        affected_actor_keys_df,
        "actor_did",
        "inner",
    )

    return affected_gold_dim_posts_df, affected_gold_dim_actors_df


def main() -> None:
    """Xác định affected keys cho Gold dimension incremental refresh.

    Input chính là local state file và các bảng Silver Iceberg.
    Output là số lượng affected post/actor keys để kiểm chứng scope trước khi
    recompute và merge Gold dimensions.
    """
    args = parse_args()
    last_successful_run_at = (
        None
        if args.ignore_state
        else read_last_successful_run_at(DEFAULT_STATE_PATH)
    )
    refresh_to = utc_now()
    refresh_window = build_refresh_window(
        last_successful_run_at=last_successful_run_at,
        refresh_to=refresh_to,
    )

    print("gold_dimensions_incremental_refresh")
    print(f"state_path: {DEFAULT_STATE_PATH}")
    print(f"last_successful_run_at: {last_successful_run_at}")
    print(f"refresh_from: {refresh_window.refresh_from.isoformat()}")
    print(f"refresh_to: {refresh_window.refresh_to.isoformat()}")
    print(f"lookback_hours: {refresh_window.lookback_hours}")

    spark = create_iceberg_spark_session("bluesky-refresh-gold-dimensions-incremental")
    spark.sparkContext.setLogLevel("WARN")

    try:
        ensure_gold_namespace(spark)
        incremental_silver_tables = read_incremental_silver_tables(
            spark,
            refresh_window,
        )
        affected_post_keys_df = build_affected_post_keys(
            incremental_silver_tables
        ).cache()
        affected_downstream_post_keys_df = build_affected_downstream_post_keys(
            incremental_silver_tables
        ).cache()
        affected_actor_keys_df = build_affected_actor_keys(
            incremental_silver_tables
        ).cache()

        print_key_samples(
            "affected_post_uri_for_dimension",
            affected_post_keys_df,
            "post_uri",
        )
        print_key_samples(
            "affected_post_uri_for_downstream_marts",
            affected_downstream_post_keys_df,
            "post_uri",
        )
        print_key_samples(
            "affected_actor_did",
            affected_actor_keys_df,
            "actor_did",
        )

        full_silver_tables = read_full_silver_tables(spark)
        affected_gold_dim_posts_df, affected_gold_dim_actors_df = (
            build_affected_dimension_rows(
                full_silver_tables,
                affected_post_keys_df,
                affected_actor_keys_df,
            )
        )
        print_dimension_key_metrics(
            "gold_dim_posts",
            affected_gold_dim_posts_df,
            "post_uri",
        )
        print_dimension_key_metrics(
            "gold_dim_actors",
            affected_gold_dim_actors_df,
            "actor_did",
        )

        for table_name, key_column, columns in GOLD_DIM_SPECS:
            source_df = (
                affected_gold_dim_posts_df
                if table_name == "gold_dim_posts"
                else affected_gold_dim_actors_df
            )
            merge_dimension_rows(
                spark,
                table_name,
                source_df,
                key_column,
                columns,
            )

        check_gold_dimension_keys(spark)
        write_last_successful_run_at(DEFAULT_STATE_PATH, refresh_to)
        print("state_updated: true")

    finally:
        for table_df in locals().get("incremental_silver_tables", {}).values():
            table_df.unpersist()
        for table_df in locals().get("full_silver_tables", {}).values():
            table_df.unpersist()
        for key_df in [
            locals().get("affected_post_keys_df"),
            locals().get("affected_downstream_post_keys_df"),
            locals().get("affected_actor_keys_df"),
        ]:
            if key_df is not None:
                key_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
