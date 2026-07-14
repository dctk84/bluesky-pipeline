"""Schema Spark dùng để parse Bronze event envelope."""

from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SUBJECT_OBJECT_SCHEMA = StructType(
    [
        StructField("uri", StringType()),
        StructField("cid", StringType()),
    ]
)

REPLY_REF_SCHEMA = StructType(
    [
        StructField("uri", StringType()),
        StructField("cid", StringType()),
    ]
)

REPLY_SCHEMA = StructType(
    [
        StructField("root", REPLY_REF_SCHEMA),
        StructField("parent", REPLY_REF_SCHEMA),
    ]
)

POST_RECORD_SCHEMA = StructType(
    [
        StructField("$type", StringType()),
        StructField("createdAt", StringType()),
        StructField("text", StringType()),
        StructField("reply", REPLY_SCHEMA),
    ]
)

ENGAGEMENT_RECORD_SCHEMA = StructType(
    [
        StructField("$type", StringType()),
        StructField("createdAt", StringType()),
        StructField("subject", SUBJECT_OBJECT_SCHEMA),
    ]
)

FOLLOW_RECORD_SCHEMA = StructType(
    [
        StructField("$type", StringType()),
        StructField("createdAt", StringType()),
        StructField("subject", StringType()),
    ]
)


def build_commit_envelope_schema(record_schema: StructType | None = None) -> StructType:
    """Tạo schema Spark cho event envelope chứa commit payload.

    Input chính là schema của commit.record; nếu None thì chỉ parse commit metadata.
    Output là StructType dùng với from_json(message_value).
    """
    commit_fields = [
        StructField("operation", StringType()),
        StructField("collection", StringType()),
        StructField("rkey", StringType()),
        StructField("cid", StringType()),
        StructField("rev", StringType()),
    ]

    if record_schema is not None:
        commit_fields.append(StructField("record", record_schema))

    commit_schema = StructType(commit_fields)

    payload_schema = StructType(
        [
            StructField("did", StringType()),
            StructField("time_us", LongType()),
            StructField("kind", StringType()),
            StructField("commit", commit_schema),
        ]
    )

    return StructType(
        [
            StructField("schema_version", LongType()),
            StructField("source", StringType()),
            StructField("event_kind", StringType()),
            StructField("received_at", StringType()),
            StructField("collection", StringType()),
            StructField("operation", StringType()),
            StructField("repository_did", StringType()),
            StructField("jetstream_time_us", LongType()),
            StructField("payload", payload_schema),
        ]
    )


BRONZE_COMMIT_EVENTS_PARQUET_SCHEMA = StructType(
    [
        StructField("message_key", StringType()),
        StructField("message_value", StringType()),
        StructField("schema_version", LongType()),
        StructField("source", StringType()),
        StructField("event_kind", StringType()),
        StructField("received_at", StringType()),
        StructField("operation", StringType()),
        StructField("repository_did", StringType()),
        StructField("jetstream_time_us", LongType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("ingest_date", DateType()),
        StructField("ingest_hour", StringType()),
        StructField("collection", StringType()),
    ]
)
