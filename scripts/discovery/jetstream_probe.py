import asyncio
import json
from pathlib import Path

import websockets

from bluesky_pipeline.transforms.event_envelope import build_event_envelope

WANTED_COLLECTIONS = [
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
]

JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe?"
    + "&".join(f"wantedCollections={collection}" for collection in WANTED_COLLECTIONS)
)

OUTPUT_PATH = Path("data/probe/jetstream_sample.jsonl")


async def main() -> None:
    """Thu thập sample Jetstream nhỏ và ghi event envelope dạng JSONL."""
    event_count = 0

    collection_counts: dict[str, int] = {}
    operation_counts: dict[str, int] = {}

    # Đảm bảo thư mục output local tồn tại trước khi ghi sample.
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("a", encoding="utf-8") as output_file:
        # Kết nối Jetstream và chỉ stream các collection thuộc scope dự án.
        async with websockets.connect(JETSTREAM_URL) as websocket:
            async for message in websocket:
                # Decode một Jetstream message rồi bọc bằng event envelope.
                event = json.loads(message)
                envelope = build_event_envelope(event)

                # Ghi mỗi envelope trên một dòng để Python và Spark đọc lại dễ hơn.
                output_file.write(json.dumps(envelope, ensure_ascii=False) + "\n")

                event_count += 1

                # Cập nhật bộ đếm nhẹ để kiểm tra phân bố event quan sát được.
                commit = event.get("commit", {})
                collection = commit.get("collection")
                collection_counts[collection] = collection_counts.get(collection, 0) + 1

                operation = commit.get("operation")
                operation_key = f"{collection}:{operation}"
                operation_counts[operation_key] = operation_counts.get(operation_key, 0) + 1

                # Dừng sau một sample hữu hạn để bước discovery local chạy nhanh.
                if event_count >= 1000:
                    print("collection_counts: ", collection_counts)
                    print("operation_counts: ", operation_counts)
                    break


if __name__ == "__main__":
    asyncio.run(main())
