"""Transformation dùng chung để build Gold modeled v1 từ Silver Iceberg."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    count,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    sha2,
    to_timestamp,
    when,
)


def _event_time(record_created_at_column: str | None = "record_created_at"):
    """Tạo expression event time ưu tiên thời gian trong record rồi tới received_at."""
    received_at_ts = to_timestamp(col("received_at"))

    if record_created_at_column is None:
        return received_at_ts

    return coalesce(to_timestamp(col(record_created_at_column)), received_at_ts)


def _latest_row_by_key(
    source_df: DataFrame,
    key_column: str,
    tie_breaker_column: str = "received_at",
) -> DataFrame:
    """Lấy dòng mới nhất theo key dựa trên jetstream_time_us và tie breaker.

    Input chính là DataFrame event-level, key cần deduplicate và cột phụ để phá
    hòa khi event time bằng nhau.
    Output là DataFrame chỉ còn một dòng mới nhất cho mỗi key.
    """
    # Gold dimension cần trạng thái mới nhất của entity, không giữ mọi update.
    latest_window = Window.partitionBy(key_column).orderBy(
        col("jetstream_time_us").desc_nulls_last(),
        col(tie_breaker_column).desc_nulls_last(),
    )

    return (
        source_df.withColumn("_gold_row_number", row_number().over(latest_window))
        .filter(col("_gold_row_number") == 1)
        .drop("_gold_row_number")
    )


def _union_by_name(dataframes: list[DataFrame]) -> DataFrame:
    """Union nhiều DataFrame cùng schema theo tên cột."""
    return reduce(lambda left, right: left.unionByName(right), dataframes)


def build_gold_dim_actors(silver_tables: Mapping[str, DataFrame]) -> DataFrame:
    """Build dimension actor từ tất cả Silver tables có chứa DID.

    Input chính là mapping các Silver DataFrame.
    Output là một dòng cho mỗi actor DID đã xuất hiện trong dữ liệu.
    """
    posts_df = silver_tables["silver_posts"]
    engagements_df = silver_tables["silver_engagements"]
    follows_df = silver_tables["silver_follows"]
    deleted_df = silver_tables["silver_deleted_records"]

    # Actor có thể là author, người tương tác, người follow hoặc target được follow.
    actor_events = _union_by_name(
        [
            posts_df.select(
                col("author_did").alias("actor_did"),
                _event_time().alias("seen_at"),
                col("ingest_date"),
            ),
            engagements_df.select(
                col("actor_did"),
                _event_time().alias("seen_at"),
                col("ingest_date"),
            ),
            follows_df.select(
                col("actor_did"),
                _event_time().alias("seen_at"),
                col("ingest_date"),
            ),
            follows_df.select(
                col("target_actor_did").alias("actor_did"),
                _event_time().alias("seen_at"),
                col("ingest_date"),
            ),
            deleted_df.select(
                col("repository_did").alias("actor_did"),
                _event_time(record_created_at_column=None).alias("seen_at"),
                col("ingest_date"),
            ),
        ]
    ).filter(col("actor_did").isNotNull())

    return actor_events.groupBy("actor_did").agg(
        spark_min("seen_at").alias("first_seen_at"),
        spark_max("seen_at").alias("last_seen_at"),
        spark_min("ingest_date").alias("first_ingest_date"),
        spark_max("ingest_date").alias("last_ingest_date"),
        count("*").alias("source_event_count"),
    )


def build_gold_dim_posts(silver_tables: Mapping[str, DataFrame]) -> DataFrame:
    """Build dimension post, đại diện trạng thái mới nhất của mỗi post_uri.

    Input chính là Silver posts và Silver deleted records.
    Output là một dòng cho mỗi post_uri mà lakehouse biết.
    """
    posts_df = silver_tables["silver_posts"]
    deleted_posts_df = silver_tables["silver_deleted_records"].filter(
        col("collection") == "app.bsky.feed.post"
    )

    latest_posts_df = _latest_row_by_key(posts_df, "post_uri").select(
        col("post_uri"),
        col("author_did"),
        col("cid"),
        col("text"),
        col("record_created_at"),
        col("text_length"),
        col("is_reply"),
        col("reply_root_uri"),
        col("reply_parent_uri"),
    )
    latest_deletes_df = _latest_row_by_key(
        deleted_posts_df.withColumnRenamed("record_uri", "post_uri"),
        "post_uri",
    )

    # first/last_seen_at tính trên cả create/update/delete để phản ánh lifecycle.
    lifecycle_events_df = _union_by_name(
        [
            posts_df.select(
                col("post_uri"),
                _event_time().alias("seen_at"),
                col("ingest_date"),
                col("ingest_hour"),
            ),
            deleted_posts_df.select(
                col("record_uri").alias("post_uri"),
                _event_time(record_created_at_column=None).alias("seen_at"),
                col("ingest_date"),
                col("ingest_hour"),
            ),
        ]
    )

    lifecycle_rollup_df = lifecycle_events_df.groupBy("post_uri").agg(
        spark_min("seen_at").alias("first_seen_at"),
        spark_max("seen_at").alias("last_seen_at"),
        spark_max("ingest_date").alias("ingest_date"),
        spark_max("ingest_hour").alias("ingest_hour"),
    )

    return (
        lifecycle_rollup_df.join(latest_posts_df, "post_uri", "left")
        .join(
            latest_deletes_df.select(
                col("post_uri"),
                _event_time(record_created_at_column=None).alias("deleted_at"),
            ),
            "post_uri",
            "left",
        )
        .select(
            col("post_uri"),
            col("author_did"),
            col("cid").alias("post_cid"),
            col("text").alias("post_text"),
            to_timestamp(col("record_created_at")).alias("post_created_at"),
            col("text_length"),
            coalesce(col("is_reply"), lit(False)).alias("is_reply"),
            col("reply_root_uri"),
            col("reply_parent_uri"),
            col("deleted_at").isNotNull().alias("is_deleted"),
            col("deleted_at"),
            col("first_seen_at"),
            col("last_seen_at"),
            col("ingest_date"),
            col("ingest_hour"),
        )
    )


def build_gold_fact_content_events(
    silver_tables: Mapping[str, DataFrame],
) -> DataFrame:
    """Build fact content events cho post lifecycle.

    Input chính là Silver posts và Silver deleted records.
    Output là một dòng cho mỗi create/update/delete event liên quan tới post.
    """
    posts_df = silver_tables["silver_posts"]
    deleted_posts_df = silver_tables["silver_deleted_records"].filter(
        col("collection") == "app.bsky.feed.post"
    )

    latest_post_state_df = _latest_row_by_key(posts_df, "post_uri").select(
        col("post_uri"),
        col("author_did"),
        col("is_reply"),
        col("reply_root_uri"),
        col("reply_parent_uri"),
    )

    post_events_df = posts_df.withColumn(
        "content_event_type",
        when(
            col("is_reply") & (col("operation") == "create"),
            lit("reply_create"),
        )
        .when(
            col("is_reply") & (col("operation") == "update"),
            lit("reply_update"),
        )
        .when(col("operation") == "create", lit("post_create"))
        .otherwise(lit("post_update")),
    ).select(
        sha2(
            concat_ws(
                "||",
                col("post_uri"),
                col("content_event_type"),
                col("jetstream_time_us").cast("string"),
            ),
            256,
        ).alias("content_event_id"),
        col("post_uri").alias("content_uri"),
        col("author_did"),
        lit("post").alias("content_type"),
        col("content_event_type"),
        col("post_uri"),
        col("is_reply"),
        col("reply_root_uri"),
        col("reply_parent_uri"),
        _event_time().alias("event_time"),
        col("received_at"),
        col("jetstream_time_us"),
        col("ingest_date"),
        col("ingest_hour"),
    )

    # Delete event thiếu record body, nên join lại latest post state nếu đã từng
    # thấy post trước đó để biết đây là post gốc hay reply.
    delete_events_df = (
        deleted_posts_df.join(
            latest_post_state_df,
            deleted_posts_df.record_uri == latest_post_state_df.post_uri,
            "left",
        )
        .withColumn(
            "content_event_type",
            when(coalesce(col("is_reply"), lit(False)), lit("reply_delete")).otherwise(
                lit("post_delete")
            ),
        )
        .select(
            sha2(
                concat_ws(
                    "||",
                    col("record_uri"),
                    col("content_event_type"),
                    col("jetstream_time_us").cast("string"),
                ),
                256,
            ).alias("content_event_id"),
            col("record_uri").alias("content_uri"),
            col("repository_did").alias("author_did"),
            lit("post").alias("content_type"),
            col("content_event_type"),
            col("record_uri").alias("post_uri"),
            coalesce(col("is_reply"), lit(False)).alias("is_reply"),
            col("reply_root_uri"),
            col("reply_parent_uri"),
            _event_time(record_created_at_column=None).alias("event_time"),
            col("received_at"),
            col("jetstream_time_us"),
            col("ingest_date"),
            col("ingest_hour"),
        )
    )

    return post_events_df.unionByName(delete_events_df)


def build_gold_fact_engagement_events(
    silver_tables: Mapping[str, DataFrame],
) -> DataFrame:
    """Build fact engagement events từ like/repost create events.

    Input chính là Silver engagements.
    Output là một dòng cho mỗi engagement event.
    """
    engagements_df = silver_tables["silver_engagements"]

    return engagements_df.select(
        col("engagement_uri").alias("engagement_event_id"),
        col("engagement_uri"),
        col("actor_did"),
        col("subject_uri").alias("target_post_uri"),
        col("subject_cid").alias("target_cid"),
        col("engagement_type"),
        concat_ws("_", col("engagement_type"), lit("create")).alias(
            "engagement_event_type"
        ),
        _event_time().alias("event_time"),
        col("received_at"),
        col("jetstream_time_us"),
        col("ingest_date"),
        col("ingest_hour"),
    )


def build_gold_fact_network_events(
    silver_tables: Mapping[str, DataFrame],
) -> DataFrame:
    """Build fact network events từ follow create/delete events.

    Input chính là Silver follows và Silver deleted records.
    Output là một dòng cho mỗi follow lifecycle event.
    """
    follows_df = silver_tables["silver_follows"]
    deleted_follows_df = silver_tables["silver_deleted_records"].filter(
        col("collection") == "app.bsky.graph.follow"
    )

    follow_lookup_df = _latest_row_by_key(follows_df, "follow_uri").select(
        col("follow_uri"),
        col("target_actor_did"),
    )

    follow_create_df = follows_df.select(
        sha2(
            concat_ws(
                "||",
                col("follow_uri"),
                lit("follow_create"),
                col("jetstream_time_us").cast("string"),
            ),
            256,
        ).alias("network_event_id"),
        col("follow_uri"),
        col("actor_did"),
        col("target_actor_did"),
        lit("follow_create").alias("network_event_type"),
        _event_time().alias("event_time"),
        col("received_at"),
        col("jetstream_time_us"),
        col("ingest_date"),
        col("ingest_hour"),
    )

    follow_delete_df = (
        deleted_follows_df.join(
            follow_lookup_df,
            deleted_follows_df.record_uri == follow_lookup_df.follow_uri,
            "left",
        )
        .select(
            sha2(
                concat_ws(
                    "||",
                    col("record_uri"),
                    lit("follow_delete"),
                    col("jetstream_time_us").cast("string"),
                ),
                256,
            ).alias("network_event_id"),
            col("record_uri").alias("follow_uri"),
            col("repository_did").alias("actor_did"),
            col("target_actor_did"),
            lit("follow_delete").alias("network_event_type"),
            _event_time(record_created_at_column=None).alias("event_time"),
            col("received_at"),
            col("jetstream_time_us"),
            col("ingest_date"),
            col("ingest_hour"),
        )
    )

    return follow_create_df.unionByName(follow_delete_df)


GOLD_TRANSFORMATIONS: dict[
    str,
    Callable[[Mapping[str, DataFrame]], DataFrame],
] = {
    # Registry này cho phép build/check Gold modeled iterate theo table contract.
    "gold_dim_actors": build_gold_dim_actors,
    "gold_dim_posts": build_gold_dim_posts,
    "gold_fact_content_events": build_gold_fact_content_events,
    "gold_fact_engagement_events": build_gold_fact_engagement_events,
    "gold_fact_network_events": build_gold_fact_network_events,
}
