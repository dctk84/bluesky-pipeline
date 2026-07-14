"""Transformation dùng chung cho Gold analytics/serving marts."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    countDistinct,
    date_trunc,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    to_date,
    unix_timestamp,
    when,
)


def build_gold_post_performance(
    gold_dim_posts_df: DataFrame,
    gold_fact_engagement_events_df: DataFrame,
) -> DataFrame:
    """Build serving mart phân tích performance từng post.

    Input chính là Gold dimension posts và Gold fact engagement events.
    Output là một dòng cho mỗi post_uri cùng các metric engagement chất lượng.
    """
    # Gom engagement theo post để giữ grain cuối cùng là một dòng cho mỗi post_uri.
    valid_engagements_df = gold_fact_engagement_events_df.filter(
        col("target_post_uri").isNotNull()
    )

    engagement_rollup_df = valid_engagements_df.groupBy("target_post_uri").agg(
        spark_sum(
            when(col("engagement_type") == "like", lit(1)).otherwise(lit(0))
        ).alias("like_count"),
        spark_sum(
            when(col("engagement_type") == "repost", lit(1)).otherwise(lit(0))
        ).alias("repost_count"),
        count(lit(1)).alias("engagement_count"),
        countDistinct("actor_did").alias("engagement_actor_count"),
        spark_min("event_time").alias("first_engagement_at"),
        spark_max("event_time").alias("last_engagement_at"),
    )

    # Có engagement trỏ tới post mà pipeline chưa thấy post create trong Bronze.
    # Giữ các post này giúp metric engagement không bị rơi khỏi serving mart.
    base_posts_df = gold_dim_posts_df.select("post_uri").unionByName(
        valid_engagements_df.select(col("target_post_uri").alias("post_uri"))
    ).distinct()

    post_dimension_df = base_posts_df.join(gold_dim_posts_df, "post_uri", "left")

    joined_df = post_dimension_df.join(
        engagement_rollup_df,
        col("post_uri") == engagement_rollup_df.target_post_uri,
        "left",
    )

    like_count = coalesce(col("like_count"), lit(0))
    repost_count = coalesce(col("repost_count"), lit(0))
    engagement_count = coalesce(col("engagement_count"), lit(0))

    # Các duration chỉ có nghĩa khi cả hai mốc thời gian đều tồn tại.
    post_lifetime_seconds = when(
        col("deleted_at").isNotNull() & col("post_created_at").isNotNull(),
        unix_timestamp("deleted_at") - unix_timestamp("post_created_at"),
    )
    time_to_first_engagement_seconds = when(
        col("first_engagement_at").isNotNull() & col("post_created_at").isNotNull(),
        unix_timestamp("first_engagement_at") - unix_timestamp("post_created_at"),
    )
    repost_to_like_ratio = when(
        like_count > 0,
        repost_count.cast("double") / like_count.cast("double"),
    )

    return joined_df.select(
        col("post_uri"),
        col("author_did"),
        col("post_created_at"),
        col("is_reply"),
        col("reply_root_uri"),
        col("reply_parent_uri"),
        col("text_length"),
        col("is_deleted"),
        col("deleted_at"),
        post_lifetime_seconds.alias("post_lifetime_seconds"),
        like_count.cast("long").alias("like_count"),
        repost_count.cast("long").alias("repost_count"),
        engagement_count.cast("long").alias("engagement_count"),
        coalesce(col("engagement_actor_count"), lit(0))
        .cast("long")
        .alias("engagement_actor_count"),
        col("first_engagement_at"),
        col("last_engagement_at"),
        time_to_first_engagement_seconds.alias("time_to_first_engagement_seconds"),
        repost_to_like_ratio.alias("repost_to_like_ratio"),
        (like_count + repost_count * lit(2)).cast("long").alias("engagement_score"),
    )


def build_gold_content_quality_hourly(
    gold_fact_content_events_df: DataFrame,
    gold_dim_posts_df: DataFrame,
) -> DataFrame:
    """Build hourly mart phân tích content quality và lifecycle.

    Input chính là Gold fact content events và Gold dimension posts.
    Output là một dòng cho mỗi giờ event time với các metric content lifecycle.
    """
    post_text_length_df = gold_dim_posts_df.select("post_uri", "text_length")

    content_with_post_df = gold_fact_content_events_df.join(
        post_text_length_df,
        "post_uri",
        "left",
    ).withColumn("window_start", date_trunc("hour", col("event_time")))

    # Avg text length chỉ tính trên create events vì update/delete không đại diện
    # cho một nội dung mới trong giờ đó.
    hourly_counts_df = content_with_post_df.groupBy("window_start").agg(
        spark_sum(
            when(col("content_event_type") == "post_create", lit(1)).otherwise(lit(0))
        ).alias("original_post_create_count"),
        spark_sum(
            when(col("content_event_type") == "reply_create", lit(1)).otherwise(lit(0))
        ).alias("reply_create_count"),
        spark_sum(
            when(col("content_event_type") == "post_update", lit(1)).otherwise(lit(0))
        ).alias("post_update_count"),
        spark_sum(
            when(col("content_event_type") == "reply_update", lit(1)).otherwise(lit(0))
        ).alias("reply_update_count"),
        spark_sum(
            when(col("content_event_type") == "post_delete", lit(1)).otherwise(lit(0))
        ).alias("post_delete_count"),
        spark_sum(
            when(col("content_event_type") == "reply_delete", lit(1)).otherwise(lit(0))
        ).alias("reply_delete_count"),
        count(lit(1)).alias("total_content_events"),
        avg(
            when(
                col("content_event_type").isin("post_create", "reply_create"),
                col("text_length").cast("double"),
            )
        ).alias("avg_text_length"),
    )

    create_count = col("original_post_create_count") + col("reply_create_count")
    delete_count = col("post_delete_count") + col("reply_delete_count")
    update_count = col("post_update_count") + col("reply_update_count")

    return hourly_counts_df.select(
        col("window_start"),
        col("original_post_create_count").cast("long"),
        col("reply_create_count").cast("long"),
        col("post_update_count").cast("long"),
        col("reply_update_count").cast("long"),
        col("post_delete_count").cast("long"),
        col("reply_delete_count").cast("long"),
        col("total_content_events").cast("long"),
        when(create_count > 0, col("reply_create_count").cast("double") / create_count)
        .otherwise(lit(None))
        .alias("reply_ratio"),
        when(
            col("total_content_events") > 0,
            delete_count.cast("double") / col("total_content_events").cast("double"),
        )
        .otherwise(lit(None))
        .alias("delete_ratio"),
        when(
            col("total_content_events") > 0,
            update_count.cast("double") / col("total_content_events").cast("double"),
        )
        .otherwise(lit(None))
        .alias("update_ratio"),
        col("avg_text_length"),
    )


def build_gold_thread_conversation_summary(
    gold_dim_posts_df: DataFrame,
    gold_fact_content_events_df: DataFrame,
) -> DataFrame:
    """Build mart phân tích conversation theo từng reply root.

    Input chính là Gold dimension posts và Gold fact content events.
    Output là một dòng cho mỗi reply_root_uri đã observe được.
    """
    reply_create_events_df = gold_fact_content_events_df.filter(
        (col("content_event_type") == "reply_create")
        & col("reply_root_uri").isNotNull()
    ).select(
        col("post_uri").alias("reply_post_uri"),
        col("reply_root_uri"),
        col("author_did").alias("reply_author_did"),
        col("event_time").alias("reply_created_at"),
    )

    reply_delete_counts_df = gold_fact_content_events_df.filter(
        (col("content_event_type") == "reply_delete")
        & col("reply_root_uri").isNotNull()
    ).groupBy("reply_root_uri").agg(
        count(lit(1)).alias("deleted_reply_count")
    )

    reply_text_df = gold_dim_posts_df.select(
        col("post_uri").alias("reply_post_uri"),
        col("text_length").alias("reply_text_length"),
    )

    # Root post có thể không nằm trong cửa sổ dữ liệu observe được, nên join left.
    root_posts_df = gold_dim_posts_df.select(
        col("post_uri").alias("reply_root_uri"),
        col("author_did").alias("root_author_did"),
        col("post_created_at").alias("root_post_created_at"),
    )

    reply_rollup_df = reply_create_events_df.join(
        reply_text_df,
        "reply_post_uri",
        "left",
    ).groupBy("reply_root_uri").agg(
        count(lit(1)).alias("reply_count"),
        countDistinct("reply_author_did").alias("reply_author_count"),
        spark_min("reply_created_at").alias("first_reply_at"),
        spark_max("reply_created_at").alias("last_reply_at"),
        avg(col("reply_text_length").cast("double")).alias("avg_reply_text_length"),
    )

    joined_df = (
        reply_rollup_df.join(reply_delete_counts_df, "reply_root_uri", "left")
        .join(root_posts_df, "reply_root_uri", "left")
    )

    return joined_df.select(
        col("reply_root_uri"),
        col("root_author_did"),
        col("root_post_created_at"),
        col("reply_count").cast("long"),
        col("reply_author_count").cast("long"),
        col("first_reply_at"),
        col("last_reply_at"),
        when(
            col("root_post_created_at").isNotNull() & col("last_reply_at").isNotNull(),
            unix_timestamp("last_reply_at") - unix_timestamp("root_post_created_at"),
        ).alias("conversation_duration_seconds"),
        col("avg_reply_text_length"),
        coalesce(col("deleted_reply_count"), lit(0))
        .cast("long")
        .alias("deleted_reply_count"),
    )


def build_gold_actor_activity_daily(
    gold_dim_actors_df: DataFrame,
    gold_dim_posts_df: DataFrame,
    gold_fact_content_events_df: DataFrame,
    gold_fact_engagement_events_df: DataFrame,
    gold_fact_network_events_df: DataFrame,
) -> DataFrame:
    """Build mart phân tích hoạt động actor theo ngày.

    Input chính là Gold modeled actor/post dimensions và các content,
    engagement, network facts.
    Output là một dòng cho mỗi `activity_date + actor_did`.
    """
    known_actors_df = gold_dim_actors_df.select("actor_did").distinct()

    content_daily_df = (
        gold_fact_content_events_df.filter(col("author_did").isNotNull())
        .withColumn("activity_date", to_date(col("event_time")))
        .groupBy("activity_date", "author_did")
        .agg(
            spark_sum(
                when(col("content_event_type") == "post_create", lit(1)).otherwise(
                    lit(0)
                )
            ).alias("original_posts_created"),
            spark_sum(
                when(col("content_event_type") == "reply_create", lit(1)).otherwise(
                    lit(0)
                )
            ).alias("replies_created"),
            spark_sum(
                when(
                    col("content_event_type").isin("post_update", "reply_update"),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("posts_updated"),
            spark_sum(
                when(
                    col("content_event_type").isin("post_delete", "reply_delete"),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("posts_deleted"),
        )
        .withColumnRenamed("author_did", "actor_did")
    )

    engagement_given_daily_df = (
        gold_fact_engagement_events_df.filter(col("actor_did").isNotNull())
        .withColumn("activity_date", to_date(col("event_time")))
        .groupBy("activity_date", "actor_did")
        .agg(
            spark_sum(
                when(col("engagement_type") == "like", lit(1)).otherwise(lit(0))
            ).alias("likes_given"),
            spark_sum(
                when(col("engagement_type") == "repost", lit(1)).otherwise(lit(0))
            ).alias("reposts_given"),
            count(lit(1)).alias("engagements_given"),
            countDistinct("target_post_uri").alias("unique_posts_engaged"),
        )
    )

    network_daily_df = (
        gold_fact_network_events_df.filter(col("actor_did").isNotNull())
        .withColumn("activity_date", to_date(col("event_time")))
        .groupBy("activity_date", "actor_did")
        .agg(
            spark_sum(
                when(col("network_event_type") == "follow_create", lit(1)).otherwise(
                    lit(0)
                )
            ).alias("follows_created"),
            spark_sum(
                when(col("network_event_type") == "follow_delete", lit(1)).otherwise(
                    lit(0)
                )
            ).alias("follows_deleted"),
            countDistinct(
                when(
                    col("network_event_type") == "follow_create",
                    col("target_actor_did"),
                )
            ).alias("unique_actors_followed"),
        )
    )

    post_authors_df = gold_dim_posts_df.select(
        col("post_uri").alias("target_post_uri"),
        col("author_did").alias("received_actor_did"),
    ).filter(col("received_actor_did").isNotNull())

    received_engagement_daily_df = (
        gold_fact_engagement_events_df.join(
            post_authors_df,
            "target_post_uri",
            "inner",
        )
        .withColumn("activity_date", to_date(col("event_time")))
        .groupBy("activity_date", "received_actor_did")
        .agg(
            spark_sum(
                when(col("engagement_type") == "like", lit(1)).otherwise(lit(0))
            ).alias("received_likes"),
            spark_sum(
                when(col("engagement_type") == "repost", lit(1)).otherwise(lit(0))
            ).alias("received_reposts"),
            count(lit(1)).alias("received_engagements"),
        )
        .withColumnRenamed("received_actor_did", "actor_did")
    )

    # Base grain lấy từ mọi activity observed được, rồi constrain bằng actor dim.
    activity_keys_df = (
        content_daily_df.select("activity_date", "actor_did")
        .unionByName(engagement_given_daily_df.select("activity_date", "actor_did"))
        .unionByName(network_daily_df.select("activity_date", "actor_did"))
        .unionByName(received_engagement_daily_df.select("activity_date", "actor_did"))
        .distinct()
        .join(known_actors_df, "actor_did", "inner")
    )

    joined_df = (
        activity_keys_df.join(content_daily_df, ["activity_date", "actor_did"], "left")
        .join(engagement_given_daily_df, ["activity_date", "actor_did"], "left")
        .join(network_daily_df, ["activity_date", "actor_did"], "left")
        .join(received_engagement_daily_df, ["activity_date", "actor_did"], "left")
    )

    original_posts_created = coalesce(col("original_posts_created"), lit(0))
    replies_created = coalesce(col("replies_created"), lit(0))
    posts_updated = coalesce(col("posts_updated"), lit(0))
    posts_deleted = coalesce(col("posts_deleted"), lit(0))
    likes_given = coalesce(col("likes_given"), lit(0))
    reposts_given = coalesce(col("reposts_given"), lit(0))
    follows_created = coalesce(col("follows_created"), lit(0))
    follows_deleted = coalesce(col("follows_deleted"), lit(0))
    engagements_given = coalesce(col("engagements_given"), lit(0))
    received_likes = coalesce(col("received_likes"), lit(0))
    received_reposts = coalesce(col("received_reposts"), lit(0))
    received_engagements = coalesce(col("received_engagements"), lit(0))
    unique_posts_engaged = coalesce(col("unique_posts_engaged"), lit(0))
    unique_actors_followed = coalesce(col("unique_actors_followed"), lit(0))

    content_events_created = original_posts_created + replies_created
    activity_score = (
        content_events_created * lit(2)
        + engagements_given
        + follows_created
        + received_engagements
    )

    return joined_df.select(
        col("activity_date"),
        col("actor_did"),
        original_posts_created.cast("long").alias("original_posts_created"),
        replies_created.cast("long").alias("replies_created"),
        posts_updated.cast("long").alias("posts_updated"),
        posts_deleted.cast("long").alias("posts_deleted"),
        likes_given.cast("long").alias("likes_given"),
        reposts_given.cast("long").alias("reposts_given"),
        follows_created.cast("long").alias("follows_created"),
        follows_deleted.cast("long").alias("follows_deleted"),
        engagements_given.cast("long").alias("engagements_given"),
        content_events_created.cast("long").alias("content_events_created"),
        received_likes.cast("long").alias("received_likes"),
        received_reposts.cast("long").alias("received_reposts"),
        received_engagements.cast("long").alias("received_engagements"),
        unique_posts_engaged.cast("long").alias("unique_posts_engaged"),
        unique_actors_followed.cast("long").alias("unique_actors_followed"),
        activity_score.cast("long").alias("activity_score"),
        when(
            engagements_given > 0,
            content_events_created.cast("double") / engagements_given.cast("double"),
        ).alias("creator_engager_ratio"),
    )


def build_gold_network_growth_daily(
    gold_fact_network_events_df: DataFrame,
) -> DataFrame:
    """Build mart phân tích observed network growth theo ngày.

    Input chính là Gold fact network events.
    Output là một dòng cho mỗi `activity_date + target_actor_did`.
    """
    network_with_date_df = gold_fact_network_events_df.withColumn(
        "activity_date",
        to_date(col("event_time")),
    )

    daily_counts_df = network_with_date_df.groupBy(
        "activity_date",
        "target_actor_did",
    ).agg(
        spark_sum(
            when(col("network_event_type") == "follow_create", lit(1)).otherwise(
                lit(0)
            )
        ).alias("follow_count"),
        spark_sum(
            when(col("network_event_type") == "follow_delete", lit(1)).otherwise(
                lit(0)
            )
        ).alias("unfollow_count"),
        countDistinct(
            when(col("network_event_type") == "follow_create", col("actor_did"))
        ).alias("unique_follower_count"),
        spark_min(
            when(col("network_event_type") == "follow_create", col("event_time"))
        ).alias("first_follow_at"),
        spark_max(
            when(col("network_event_type") == "follow_create", col("event_time"))
        ).alias("last_follow_at"),
    )

    follow_count = coalesce(col("follow_count"), lit(0))
    unfollow_count = coalesce(col("unfollow_count"), lit(0))

    return daily_counts_df.select(
        col("activity_date"),
        col("target_actor_did"),
        follow_count.cast("long").alias("follow_count"),
        unfollow_count.cast("long").alias("unfollow_count"),
        (follow_count - unfollow_count).cast("long").alias("net_follow_count"),
        coalesce(col("unique_follower_count"), lit(0))
        .cast("long")
        .alias("unique_follower_count"),
        col("first_follow_at"),
        col("last_follow_at"),
    )
