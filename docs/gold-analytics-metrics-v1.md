# Gold analytics metrics v1

Tài liệu này mô tả bộ metric Gold aggregate/serving v1 cho **lakehouse path**.

Mục tiêu không phải tạo thêm các metric đếm event đơn giản giống realtime fast
path. Mục tiêu là tạo các bảng metric có khả năng trả lời câu hỏi phân tích rõ
ràng hơn từ Gold modeled layer, rồi load vào ClickHouse để Grafana query nhanh.

Luồng dữ liệu mục tiêu:

```text
Gold modeled Iceberg
  -> Gold analytics / serving marts
  -> ClickHouse
  -> Grafana
```

Realtime fast path vẫn giữ nhiệm vụ riêng:

```text
Kafka -> Spark Structured Streaming -> ClickHouse realtime marts -> Grafana
```

Trạng thái hiện tại: năm serving marts v1 trong tài liệu này đã được implement,
load vào ClickHouse và đưa vào Gold incremental refresh:

```text
bluesky.gold_post_performance
bluesky.gold_content_quality_hourly
bluesky.gold_thread_conversation_summary
bluesky.gold_actor_activity_daily
bluesky.gold_network_growth_daily
```

Hai bảng `bluesky.gold_event_volume_by_type` và
`bluesky.gold_post_engagement_summary` vẫn được giữ như legacy/minimal serving
checkpoints, không phải bộ metric phân tích chính.

## 1. Nguyên tắc thiết kế

- Gold analytics/serving phải đọc từ Gold modeled Iceberg, không đọc trực tiếp từ
  Silver nếu Gold modeled đã có đủ dữ liệu.
- Mỗi bảng metric phải trả lời được một nhóm câu hỏi cụ thể.
- Metric phải có grain rõ ràng trước khi viết transformation.
- ClickHouse lưu kết quả phục vụ dashboard, không phải source of truth.
- Bảng aggregate phải rebuild được từ Gold modeled.
- Không thêm enrichment chưa có trong nguồn, ví dụ handle, display name,
  follower count ngoài dữ liệu observe được, language detection hoặc sentiment.
- Không tuyên bố exactly-once; reconciliation vẫn là cơ chế kiểm chứng chính.

## 2. Các câu hỏi phân tích cần trả lời

### 2.1. Content quality và content lifecycle

Các câu hỏi:

- Nội dung trong dữ liệu quan sát được thiên về original post hay reply?
- Tỷ lệ reply so với original post thay đổi thế nào theo thời gian?
- Post có thường bị update hoặc delete không?
- Post bị delete sau bao lâu kể từ khi xuất hiện?
- Text length có liên quan tới engagement không?

Ý nghĩa:

- Giúp hiểu nguồn dữ liệu đang phản ánh broadcast content hay conversation.
- Giúp nhận diện thời điểm hoạt động hội thoại tăng mạnh.
- Giúp theo dõi lifecycle của content thay vì chỉ đếm create event.

### 2.2. Engagement quality

Các câu hỏi:

- Post nào nhận được nhiều like/repost nhất?
- Post nhận engagement nhanh hay chậm sau khi được tạo?
- Tỷ lệ repost trên like là bao nhiêu?
- Bao nhiêu post có engagement, bao nhiêu post không nhận tương tác?
- Engagement tập trung vào một số ít post hay phân tán đều?

Ý nghĩa:

- Giúp phân biệt volume và quality. Nhiều post không nhất thiết nghĩa là nhiều
  tương tác.
- Giúp tìm content có sức lan truyền mạnh hơn, ví dụ repost ratio cao.
- Giúp tạo dashboard sâu hơn so với realtime event count.

### 2.3. Conversation dynamics

Các câu hỏi:

- Thread nào có nhiều reply nhất?
- Một thread có bao nhiêu actor tham gia?
- Conversation kéo dài bao lâu?
- Reply tập trung vào một vài root post hay rải đều?

Ý nghĩa:

- Bluesky không chỉ có post độc lập; reply tạo thành conversation.
- Thread summary giúp phân tích mức độ thảo luận, không chỉ số lượng post.

### 2.4. Actor behavior

Các câu hỏi:

- Actor nào tạo nhiều content nhất?
- Actor nào tương tác nhiều nhất qua like/repost/follow?
- Actor nào nhận nhiều engagement nhất?
- Actor nào vừa tạo content vừa tương tác?
- Hoạt động đang tập trung vào một nhóm actor nhỏ hay phân tán?

Ý nghĩa:

- Giúp phân loại hành vi actor: creator, engager, network builder.
- Giúp tạo bảng leaderboards hoặc cohort đơn giản mà không cần identity
  enrichment.

### 2.5. Network activity

Các câu hỏi:

- Actor nào được follow nhiều nhất trong dữ liệu observe được?
- Follow/unfollow tạo ra net growth thế nào theo ngày?
- Network activity có tăng cùng content/engagement không?

Ý nghĩa:

- Follow events phản ánh social graph dynamics.
- Vì project chỉ quan sát stream public trong một khoảng thời gian, metric này là
  observed network activity, không phải follower count toàn cục của Bluesky.

## 3. Bảng serving v1

### 3.1. `gold_post_performance`

**Mức ưu tiên**

Cao nhất. Đây là bảng nên implement đầu tiên.

**Grain**

Một dòng cho mỗi `post_uri` đã xuất hiện trong `gold_dim_posts` hoặc được trỏ tới
bởi `gold_fact_engagement_events.target_post_uri`.

**Nguồn Gold modeled**

- `gold_dim_posts`
- `gold_fact_engagement_events`
- `gold_fact_content_events`

**Câu hỏi trả lời**

- Post nào có performance tốt nhất?
- Original post và reply khác nhau thế nào về engagement?
- Post nhận engagement nhanh hay chậm?
- Post bị delete có engagement khác post còn tồn tại không?
- Text length có liên quan tới engagement không?

**Cột đề xuất**

```text
post_uri
author_did
post_created_at
is_reply
reply_root_uri
reply_parent_uri
text_length
is_deleted
deleted_at
post_lifetime_seconds
like_count
repost_count
engagement_count
engagement_actor_count
first_engagement_at
last_engagement_at
time_to_first_engagement_seconds
repost_to_like_ratio
engagement_score
```

**Ghi chú metric**

- `engagement_count = like_count + repost_count`.
- `engagement_actor_count` đếm số actor duy nhất đã like/repost post đó.
- `time_to_first_engagement_seconds` giúp phân tích tốc độ nhận tương tác.
- `repost_to_like_ratio` là proxy đơn giản cho khả năng lan truyền.
- Nếu post chỉ xuất hiện như target của engagement nhưng pipeline chưa observe
  được post create event, các cột metadata như `author_did`, `post_created_at`
  hoặc `text_length` có thể null. Vẫn giữ dòng này để không làm rơi engagement
  metric khỏi serving mart.
- `engagement_score` v1 có thể tính đơn giản:

```text
like_count + repost_count * 2
```

Repost được weighted cao hơn like vì repost thường làm content lan truyền rộng
hơn trong social graph.

### 3.2. `gold_content_quality_hourly`

**Mức ưu tiên**

Cao.

**Grain**

Một dòng cho mỗi giờ event time.

**Nguồn Gold modeled**

- `gold_fact_content_events`
- `gold_dim_posts`

**Câu hỏi trả lời**

- Hoạt động content theo giờ thay đổi thế nào?
- Tỷ lệ reply/original post theo giờ là bao nhiêu?
- Tỷ lệ update/delete theo giờ có bất thường không?
- Text length trung bình theo giờ thay đổi ra sao?

**Cột đề xuất**

```text
window_start
original_post_create_count
reply_create_count
post_update_count
reply_update_count
post_delete_count
reply_delete_count
total_content_events
reply_ratio
delete_ratio
update_ratio
avg_text_length
```

**Ghi chú metric**

- `window_start` dùng hour-level, ví dụ `date_trunc('hour', event_time)`.
- Bảng này khác fast path ở chỗ nó phân tích lifecycle và content mix, không chỉ
  đếm event type realtime.

### 3.3. `gold_thread_conversation_summary`

**Mức ưu tiên**

Trung bình cao.

**Grain**

Một dòng cho mỗi `reply_root_uri`.

**Nguồn Gold modeled**

- `gold_dim_posts`
- `gold_fact_content_events`

**Câu hỏi trả lời**

- Thread nào tạo nhiều conversation nhất?
- Bao nhiêu actor tham gia một thread?
- Conversation kéo dài trong bao lâu?
- Reply trong thread có bị delete nhiều không?

**Cột đề xuất**

```text
reply_root_uri
root_author_did
root_post_created_at
reply_count
reply_author_count
first_reply_at
last_reply_at
conversation_duration_seconds
avg_reply_text_length
deleted_reply_count
```

**Ghi chú metric**

- V1 chỉ tính thread dựa trên reply có `reply_root_uri`.
- Nếu root post không nằm trong dữ liệu observe được, các cột root metadata có
  thể null.

### 3.4. `gold_actor_activity_daily`

**Mức ưu tiên**

Trung bình.

**Grain**

Một dòng cho mỗi `activity_date + actor_did`.

**Nguồn Gold modeled**

- `gold_dim_actors`
- `gold_dim_posts`
- `gold_fact_content_events`
- `gold_fact_engagement_events`
- `gold_fact_network_events`

**Câu hỏi trả lời**

- Actor nào tạo nhiều content?
- Actor nào đi tương tác nhiều?
- Actor nào nhận nhiều engagement?
- Actor nào có hành vi cân bằng giữa create và engage?

**Cột đề xuất**

```text
activity_date
actor_did
original_posts_created
replies_created
posts_updated
posts_deleted
likes_given
reposts_given
follows_created
follows_deleted
engagements_given
content_events_created
received_likes
received_reposts
received_engagements
unique_posts_engaged
unique_actors_followed
activity_score
creator_engager_ratio
```

**Ghi chú metric**

- `received_*` cần join engagement target post về author trong `gold_dim_posts`.
- `activity_score` v1 có thể là weighted score đơn giản:

```text
content_events_created * 2
+ engagements_given
+ follows_created
+ received_engagements
```

- `creator_engager_ratio` giúp phân biệt actor chủ yếu tạo nội dung với actor chủ
  yếu đi tương tác.

### 3.5. `gold_network_growth_daily`

**Mức ưu tiên**

Trung bình.

**Grain**

Một dòng cho mỗi `activity_date + target_actor_did`.

**Nguồn Gold modeled**

- `gold_fact_network_events`

**Câu hỏi trả lời**

- Actor nào được follow nhiều nhất trong dữ liệu observe được?
- Net follow tăng/giảm thế nào theo ngày?
- Có actor nào có unfollow spike không?

**Cột đề xuất**

```text
activity_date
target_actor_did
follow_count
unfollow_count
net_follow_count
unique_follower_count
first_follow_at
last_follow_at
```

**Ghi chú metric**

- Đây là observed network growth, không phải follower count toàn cục.
- `target_actor_did` có thể null với unfollow nếu delete event không lookup được
  follow history; checkpoint cần theo dõi tỷ lệ null này.

## 4. Thứ tự implement đề xuất

Thứ tự nên đi từ bảng tạo nhiều insight nhất và dễ kiểm chứng nhất:

1. `gold_post_performance`
2. `gold_content_quality_hourly`
3. `gold_thread_conversation_summary`
4. `gold_actor_activity_daily`
5. `gold_network_growth_daily`

Lý do chọn `gold_post_performance` đầu tiên:

- Dùng trực tiếp `gold_dim_posts` và `gold_fact_engagement_events`, hai bảng Gold
  modeled đã có rõ nhất.
- Trả lời được câu hỏi phân tích có giá trị: content nào có engagement tốt, engagement
  đến nhanh hay chậm, reply khác original post ra sao.
- Có thể thay thế và mở rộng bảng `gold_post_engagement_summary` cũ.

## 5. Dashboard gợi ý

Các panel Grafana nên ưu tiên:

- Top posts by engagement score.
- Like vs repost mix by top posts.
- Time to first engagement distribution.
- Original post vs reply engagement comparison.
- Content lifecycle hourly: create/update/delete/reply ratio.
- Top threads by reply count.
- Top actors by activity score.
- Observed network growth by target actor.

## 6. Checkpoint và reconciliation

Mỗi serving table cần checkpoint tối thiểu:

- Row count > 0 nếu source Gold modeled có dữ liệu.
- Key không null theo grain của bảng.
- Không duplicate theo grain.
- Tổng metric quan trọng reconcile được với Gold modeled source.

Ví dụ với `gold_post_performance`:

```text
sum(like_count) == count(gold_fact_engagement_events where engagement_type = 'like')
sum(repost_count) == count(gold_fact_engagement_events where engagement_type = 'repost')
sum(engagement_count) == count(gold_fact_engagement_events)
```

## 7. Ngoài scope của v1

- Sentiment, topic modeling, language detection.
- Hashtag, URL, domain analytics.
- True follower count toàn cục của Bluesky.
- Identity enrichment như handle/display name.
- Ranking model phức tạp hoặc machine learning.
- Exactly-once guarantee end-to-end.
