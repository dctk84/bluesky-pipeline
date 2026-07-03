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
