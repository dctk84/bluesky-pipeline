# Ghi chú schema Jetstream

Tài liệu này ghi lại các quan sát ban đầu từ Milestone 1 khi đọc dữ liệu thật từ
Bluesky Jetstream.

Mục tiêu của tài liệu không phải định nghĩa schema cuối cùng, mà là ghi lại những
field đã quan sát được để phục vụ thiết kế ingestion, Kafka key, Bronze layout và
Spark normalization ở các milestone sau.

## Phạm vi đã quan sát

Các collection đang thuộc phạm vi dự án:

```text
app.bsky.feed.post
app.bsky.feed.like
app.bsky.feed.repost
app.bsky.graph.follow
```

Trong một lần chạy probe 1000 event, script ghi được sample dạng event envelope
vào JSONL local.

Số lượng event theo collection:

```text
app.bsky.feed.like: 691
app.bsky.feed.post: 120
app.bsky.feed.repost: 108
app.bsky.graph.follow: 81
```

Số lượng event theo collection và operation:

```text
app.bsky.feed.like:create: 683
app.bsky.feed.post:create: 113
app.bsky.feed.repost:create: 104
app.bsky.graph.follow:create: 61
app.bsky.graph.follow:delete: 20
app.bsky.feed.like:delete: 8
app.bsky.feed.post:delete: 7
app.bsky.feed.repost:delete: 4
```

Kết quả này xác nhận endpoint Jetstream có thể cung cấp multi-collection events
cho scope hiện tại. Sample này cũng cho thấy cả bốn collection đều có thể xuất
hiện `delete`. Chưa nên kết luận `update` không tồn tại nếu chưa quan sát đủ lâu.

Số lượng record type quan sát được:

```text
app.bsky.feed.like: 683
app.bsky.feed.post: 113
app.bsky.feed.repost: 104
app.bsky.graph.follow: 61
missing: 39
```

`missing` khớp với tổng số delete event trong sample:

```text
20 follow delete
+ 8 like delete
+ 7 post delete
+ 4 repost delete
= 39 missing record
```

Điều này cho thấy delete event trong sample không có `record` đầy đủ. Các bước
xử lý sau phải kiểm tra missing field thay vì giả định mọi event đều có record.

Shape của `record.subject` theo collection:

```text
app.bsky.feed.like: dict
app.bsky.feed.repost: dict
app.bsky.graph.follow: str
app.bsky.feed.post: null
```

Điểm này xác nhận cùng tên field `subject` nhưng shape khác nhau giữa collection,
nên Spark normalization không nên xử lý mọi collection bằng một schema phẳng duy
nhất.

Các collection này đại diện cho ba nhóm dữ liệu:

- Content activity: bài viết, reply, quote, update và delete.
- Engagement activity: like và repost.
- Network activity: follow.

## Field chung cấp event

Các event quan sát được có một số field cấp cao:

```text
kind
did
time_us
commit
```

Ý nghĩa ban đầu:

- `kind`: loại event cấp cao từ Jetstream.
- `did`: repository DID phát sinh event.
- `time_us`: timestamp từ Jetstream ở đơn vị microsecond.
- `commit`: thông tin thay đổi trong repository.

## Field chung trong commit

Các field thường gặp trong `commit`:

```text
collection
operation
rkey
record
cid
rev
```

Ý nghĩa ban đầu:

- `collection`: loại record thay đổi, ví dụ `app.bsky.feed.post`.
- `operation`: thao tác xảy ra, ví dụ `create`, `update` hoặc `delete`.
- `rkey`: record key trong repository.
- `record`: payload của record khi event có dữ liệu record.
- `cid`: content identifier của record.
- `rev`: revision của repository.

Không phải mọi operation đều có đầy đủ `record`. Delete event có thể thiếu record
đầy đủ, nên các bước xử lý sau phải kiểm tra null/missing field.

## Normalized event schema ban đầu

Sau khi có event envelope, project có thêm bước normalize nhẹ bằng
`normalize_event()`.

Mục tiêu của bước này là biến event envelope lồng nhau thành một record phẳng hơn
để dễ quan sát, dễ ghi thử nghiệm và làm tiền đề cho Spark normalization sau này.
Đây chưa phải Silver schema cuối cùng.

Script local dùng để tạo sample normalized:

```text
scripts/normalize_sample.py
```

Input local:

```text
data/probe/jetstream_sample.jsonl
```

Output local:

```text
data/probe/jetstream_normalized_sample.jsonl
```

Các field normalized hiện tại:

```text
schema_version
source
received_at
repository_did
jetstream_time_us
collection
operation
rkey
cid
record_type
record_created_at
text
subject_uri
subject_cid
raw_event
```

Ý nghĩa:

- `schema_version`: version của envelope schema nội bộ.
- `source`: nguồn dữ liệu, hiện tại là `bluesky_jetstream`.
- `received_at`: thời điểm ingestion nhận event theo UTC.
- `repository_did`: DID của repository phát sinh event.
- `jetstream_time_us`: timestamp từ Jetstream ở đơn vị microsecond.
- `collection`: collection thay đổi, ví dụ `app.bsky.feed.post`.
- `operation`: thao tác trong commit, ví dụ `create` hoặc `delete`.
- `rkey`: record key trong repository.
- `cid`: content identifier của record trong commit nếu event có field này.
- `record_type`: giá trị `record.$type` nếu event có record.
- `record_created_at`: thời điểm tạo record nếu record có `createdAt`.
- `text`: nội dung post nếu event là post create có text.
- `subject_uri`: target URI của like/repost hoặc target DID của follow.
- `subject_cid`: target CID của like/repost nếu `subject` là object.
- `raw_event`: payload gốc từ Jetstream, giữ lại để audit và replay.

Lưu ý về `subject`:

- Với `app.bsky.feed.like`, `record.subject` thường là object có `uri` và `cid`.
- Với `app.bsky.feed.repost`, `record.subject` thường là object có `uri` và `cid`.
- Với `app.bsky.graph.follow`, `record.subject` thường là DID string.
- Với `app.bsky.feed.post`, `record.subject` thường không tồn tại.

Vì vậy normalized schema tạm thời dùng:

```text
subject_uri
subject_cid
```

Trong đó `subject_uri` có thể chứa AT URI hoặc DID string tùy collection. Khi sang
Silver, có thể cần tách rõ hơn thành các field theo ngữ nghĩa như
`target_post_uri`, `target_post_cid` hoặc `target_actor_did`.

Lý do vẫn giữ `raw_event`:

- Bronze cần giữ dữ liệu gần nguồn để audit.
- Có thể replay hoặc reprocess khi logic normalize thay đổi.
- Tránh mất field chưa dùng ở giai đoạn discovery.

## app.bsky.feed.post

Post event đại diện cho content activity.

Các field cần tiếp tục khảo sát:

```text
record.$type
record.createdAt
record.text
record.reply
record.embed
record.facets
record.langs
record.bridgyOriginalText
record.bridgyOriginalUrl
record.via
```

Các tín hiệu có thể trích xuất bằng Spark:

```text
text_length
hashtag
shared_domain
is_reply
is_quote
language
```

Use case liên quan:

- Post volume theo thời gian.
- Create/update/delete activity.
- Trending hashtags.
- Top shared domains.
- Reply/quote ratio.
- Language activity.

## app.bsky.feed.like

Like event đại diện cho engagement activity.

Các field cần tiếp tục khảo sát:

```text
record.$type
record.createdAt
record.subject.uri
record.subject.cid
record.via
```

`record.subject` trong like thường là object trỏ tới post được like.

Use case liên quan:

- Like volume theo thời gian.
- Like velocity của post.
- Rapidly engaging posts.

## app.bsky.feed.repost

Repost event đại diện cho engagement activity.

Các field cần tiếp tục khảo sát:

```text
record.$type
record.createdAt
record.subject.uri
record.subject.cid
record.via
```

`record.subject` trong repost thường là object trỏ tới post được repost.

Use case liên quan:

- Repost volume theo thời gian.
- Repost velocity của post.
- Engagement velocity tổng hợp từ like và repost.

## app.bsky.graph.follow

Follow event đại diện cho network activity.

Các field cần tiếp tục khảo sát:

```text
record.$type
record.createdAt
record.subject
record.via
```

`record.subject` trong follow có thể là DID string thay vì object. Đây là điểm
quan trọng vì cùng tên field `subject` nhưng shape khác nhau giữa collection.

Use case liên quan:

- Follow events per minute.
- Network activity trend.
- Active repositories theo follow activity.

## Ứng viên key ban đầu

Kafka message key ban đầu:

```text
did
```

Lý do:

- Các event thuộc cùng repository có khả năng vào cùng partition.
- Giữ ordering tương đối trong phạm vi repository.
- Phân phối dữ liệu tốt hơn so với key theo hashtag hoặc domain.

Ứng viên định danh record:

```text
did
commit.collection
commit.rkey
```

Ứng viên định danh engagement target:

```text
record.subject.uri
record.subject.cid
```

Ứng viên định danh follow target:

```text
record.subject
```

## Kafka raw topic local

Trong Milestone 1, project đã kiểm chứng Kafka local bằng cách publish event
envelope từ sample JSONL vào raw topic.

Topic hiện tại:

```text
bluesky.raw.events.v1
```

Cấu hình local đã kiểm chứng:

```text
bootstrap server: localhost:9092
docker compose service: kafka
container name: bluesky-kafka
partitions: 3
replication factor: 1
```

Message key hiện tại:

```text
repository_did
```

Message value hiện tại:

```text
event envelope JSON
```

Lý do dùng `repository_did` làm key:

- Giữ ordering tương đối trong phạm vi repository.
- Phù hợp với cách Jetstream event gắn với repository DID.
- Tránh dùng các key quá lệch phân phối như hashtag hoặc domain ở tầng raw.

Các script đã dùng để kiểm chứng:

```text
scripts/publish_sample_to_kafka.py
scripts/publish_sample_batch_to_kafka.py
```

Luồng đã kiểm chứng:

```text
data/probe/jetstream_sample.jsonl
        -> Python Producer
        -> Kafka topic bluesky.raw.events.v1
        -> Kafka Console Consumer
```

Đây mới là kiểm chứng local cho raw event topic. Chưa có đảm bảo exactly-once,
chưa có retry policy hoàn chỉnh và chưa phải ingestion gateway production.

## Live ingestion gateway ban đầu

Sau khi kiểm chứng publish từ sample JSONL, project đã có gateway ban đầu đọc dữ
liệu live từ Jetstream và publish trực tiếp vào Kafka raw topic.

File gateway:

```text
src/bluesky_pipeline/ingestion_gateway.py
```

Luồng đã kiểm chứng:

```text
Bluesky Jetstream WebSocket
        -> Python ingestion gateway
        -> event envelope
        -> Kafka topic bluesky.raw.events.v1
        -> Kafka Console Consumer
```

Input:

```text
Jetstream WebSocket live events
```

Output:

```text
Kafka topic bluesky.raw.events.v1
```

Scope collection hiện tại:

```text
app.bsky.feed.post
app.bsky.feed.like
app.bsky.feed.repost
app.bsky.graph.follow
```

Message key:

```text
repository_did
```

Message value:

```text
event envelope JSON
```

Cấu hình environment variable hiện tại:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC
MAX_EVENTS
MAX_RETRIES
RETRY_BACKOFF_SECONDS
```

Ý nghĩa:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap server, mặc định
  `localhost:9092`.
- `KAFKA_TOPIC`: topic raw event, mặc định `bluesky.raw.events.v1`.
- `MAX_EVENTS`: số event tối đa gateway publish trong local discovery, mặc định
  `100`.
- `MAX_RETRIES`: số lần retry tối đa khi WebSocket gặp lỗi, mặc định `3`.
- `RETRY_BACKOFF_SECONDS`: số giây chờ giữa các lần retry, mặc định `5`.

`MAX_EVENTS` giúp kiểm chứng local nhanh và tránh để process chạy vô hạn trong
giai đoạn discovery.

`MAX_RETRIES` và `RETRY_BACKOFF_SECONDS` tạo bounded retry cơ bản để gateway không
dừng ngay khi WebSocket lỗi tạm thời, nhưng cũng không retry vô hạn khi nguồn lỗi
liên tục.

Các cấu hình này không phải secret, nhưng vẫn được đọc qua environment variable
để tránh hard-code theo môi trường. Khi project có secret hoặc credential, các giá
trị đó cũng phải đi qua environment variable và không được commit vào Git.

Gateway hiện tại đã dùng structured logging cơ bản thay cho `print` ở phần kết
quả chạy và lỗi delivery.

Log summary hiện tại ghi các thông tin:

```text
topic
published_events
delivery_failed
```

Mục tiêu của logging ở bước này là giúp quan sát nhanh gateway local khi chạy thủ
công. Đây chưa phải logging/metrics đầy đủ cho production.

Gateway hiện tại đã có reconnect và bounded retry cơ bản khi WebSocket gặp lỗi.
Khi retry vượt quá `MAX_RETRIES`, gateway log lỗi và dừng thay vì loop vô hạn.

Gateway hiện tại đã có graceful shutdown cơ bản. Khi process bị interrupt hoặc
task bị cancel, gateway vẫn đi qua nhánh kết thúc để gọi `flush()` cho Kafka
producer trước khi thoát. Điều này giúp giảm rủi ro mất message còn nằm trong
producer buffer, nhưng chưa thay thế được cơ chế checkpoint hoặc recovery đầy đủ.

Những phần chưa triển khai ở bản gateway đầu tiên:

- Structured logging đầy đủ cho mọi lifecycle event.
- Metrics cho throughput, delivery failure và latency.
- Backpressure handling khi Kafka hoặc downstream chậm.

Vì vậy gateway hiện tại mới chứng minh được luồng live ingestion cơ bản, chưa phải
ingestion gateway hoàn chỉnh.

## Ghi chú thiết kế

Gateway không nên normalize sâu theo từng collection. Gateway chỉ nên giữ raw
payload gần nguồn, bổ sung metadata ingestion và publish sang Kafka.

Spark mới là nơi parse multi-schema, validate, normalize, deduplicate và tạo các
bảng Silver/Gold theo use case.

Bronze cần giữ raw event để audit, replay và reprocess khi schema normalization
thay đổi.

## Sample JSONL local

Probe script có thể ghi raw event sample vào file local:

```text
data/probe/jetstream_sample.jsonl
```

File này dùng để khảo sát schema sau khi chạy probe, ví dụ kiểm tra shape của từng
collection, so sánh `record.subject` giữa like/repost/follow hoặc xem các field
thiếu theo operation.

Nguyên tắc:

- Đây là dữ liệu thật từ Jetstream, chỉ dùng trong môi trường local.
- Không commit file sample lên Git.
- Thư mục `data/` phải nằm trong `.gitignore`.
- Khi cần sample mới, có thể xóa file local và chạy lại probe.
