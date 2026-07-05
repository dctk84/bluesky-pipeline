"""Khai báo metadata dùng chung cho các path Bronze trên MinIO."""

BRONZE_COMMIT_EVENTS_PATH = "s3a://bluesky-lake/bronze/bluesky_commit_events"
BRONZE_IDENTITY_EVENTS_PATH = "s3a://bluesky-lake/bronze/bluesky_identity_events"
BRONZE_ACCOUNT_EVENTS_PATH = "s3a://bluesky-lake/bronze/bluesky_account_events"

BRONZE_EVENT_PATHS = {
    "commit": BRONZE_COMMIT_EVENTS_PATH,
    "identity": BRONZE_IDENTITY_EVENTS_PATH,
    "account": BRONZE_ACCOUNT_EVENTS_PATH,
}