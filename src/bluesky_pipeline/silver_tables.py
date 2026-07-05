"""Khai báo metadata dùng chung cho các bảng Silver v1."""

SILVER_POSTS_PATH = "s3a://bluesky-lake/silver/silver_posts"
SILVER_ENGAGEMENTS_PATH = "s3a://bluesky-lake/silver/silver_engagements"
SILVER_FOLLOWS_PATH = "s3a://bluesky-lake/silver/silver_follows"
SILVER_DELETED_RECORDS_PATH = "s3a://bluesky-lake/silver/silver_deleted_records"

SILVER_TABLE_PATHS = {
    "silver_posts": SILVER_POSTS_PATH,
    "silver_engagements": SILVER_ENGAGEMENTS_PATH,
    "silver_follows": SILVER_FOLLOWS_PATH,
    "silver_deleted_records": SILVER_DELETED_RECORDS_PATH,
}
