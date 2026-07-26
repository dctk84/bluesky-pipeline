# Silver schema v1

Tài liệu này mô tả thiết kế Silver schema ban đầu cho dữ liệu Bluesky đã ghi ở
Bronze trên MinIO.

Mục tiêu của Silver v1 là chuẩn hóa các commit event chính thành bảng dễ query,
dễ validate và làm nguồn cho Gold analytics. Schema có thể tiếp tục được điều
chỉnh khi có thêm dữ liệu và use case.

Trạng thái hiện tại: Silver v1 đã được materialize bằng Apache Iceberg trên MinIO
và là source of truth của lakehouse path cho các bảng commit event chính. Tài
liệu này vẫn giữ vai trò mô tả contract schema của Silver v1.

## Nguồn dữ liệu

Input hiện tại:

```text
s3a://bluesky-lake/bronze/bluesky_commit_events
```

Bronze commit events đã được tách khỏi:

```text
s3a://bluesky-lake/bronze/bluesky_identity_events
s3a://bluesky-lake/bronze/bluesky_account_events
```

Silver v1 xử lý commit events. Identity/account events nằm ngoài scope hiện tại
và cần thiết kế Silver riêng khi triển khai bài toán account lifecycle.

Namespace/table hiện tại:

```text
lakehouse.silver_v1.silver_posts
lakehouse.silver_v1.silver_engagements
lakehouse.silver_v1.silver_follows
lakehouse.silver_v1.silver_deleted_records
```

Script build/check liên quan:

```text
scripts/lakehouse/build_iceberg_silver_v1.py
scripts/lakehouse/stream_silver_from_bronze.py
scripts/lakehouse/check_iceberg_silver_v1.py
scripts/lakehouse/check_trino_silver_v1.py
```

## Quan sát từ Bronze profile

Profile hiện tại cho thấy:

- Like create có `rkey`, `cid`, `record_type`, `record_created_at` và
  `subject_uri`.
- Repost create có `rkey`, `cid`, `record_type`, `record_created_at` và
  `subject_uri`.
- Post create/update có `rkey`, `cid`, `record_type`, `record_created_at` và
  `text`.
- Một phần post create có `reply_root_uri`, tức là reply post.
- Follow create có `rkey`, `cid`, `record_type` và `record_created_at`, nhưng
  `subject_uri` chưa parse được bằng schema hiện tại vì `record.subject` của
  follow là string, không phải object.
- Delete event có `rkey` nhưng thường thiếu `cid`, `record_type` và
  `record_created_at`.

## Bảng Silver v1 hiện tại

### `silver_posts`

Dùng cho post create/update.

Các cột chính:

```text
post_uri
author_did
operation
rkey
cid
record_type
record_created_at
received_at
jetstream_time_us
text
text_length
is_reply
reply_root_uri
reply_parent_uri
ingest_date
ingest_hour
```

Ghi chú:

- `post_uri` có thể dựng từ `author_did`, collection và `rkey`:
  `at://{author_did}/app.bsky.feed.post/{rkey}`.
- `is_reply = reply_root_uri is not null`.
- `text_length` tính từ `text`.
- Delete post chưa ghi vào `silver_posts`; delete được đưa vào
  `silver_deleted_records`.

### `silver_engagements`

Dùng cho like/repost create.

Các cột chính:

```text
engagement_uri
actor_did
engagement_type
operation
rkey
cid
record_type
record_created_at
received_at
jetstream_time_us
subject_uri
subject_cid
ingest_date
ingest_hour
```

Ghi chú:

- `engagement_type` lấy từ collection:
  - `like` cho `app.bsky.feed.like`
  - `repost` cho `app.bsky.feed.repost`
- `engagement_uri` có thể dựng từ `actor_did`, collection và `rkey`.
- Delete like/repost chưa ghi vào bảng này; delete được đưa vào
  `silver_deleted_records`.

### `silver_follows`

Dùng cho follow create.

Các cột chính:

```text
follow_uri
actor_did
target_actor_did
operation
rkey
cid
record_type
record_created_at
received_at
jetstream_time_us
ingest_date
ingest_hour
```

Ghi chú:

- `target_actor_did` lấy từ `record.subject`.
- Với follow, `record.subject` là string nên cần parse riêng, không dùng schema
  object giống like/repost.

### `silver_deleted_records`

Dùng cho mọi delete event.

Các cột chính:

```text
record_uri
repository_did
collection
operation
rkey
received_at
jetstream_time_us
ingest_date
ingest_hour
```

Ghi chú:

- Delete event thường chỉ có `rkey`, `collection`, `operation` và repository DID.
- `record_uri` có thể dựng từ `repository_did`, collection và `rkey`.
- Bảng này giúp downstream biết record nào đã bị xóa mà không cần record payload
  đầy đủ.

## Ngoài scope của Silver v1

Các phần chưa xử lý trong Silver v1:

- Deduplication.
- Watermark và late event handling.
- Pseudonymization/hash DID.
- Quarantine invalid events.
- Account lifecycle Silver tables.
- Hashtag, language và shared domain extraction.

Các phần này nằm ngoài scope Silver v1 và có thể được bổ sung khi cần mở rộng
khả năng xử lý dữ liệu sạch.
