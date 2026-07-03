import asyncio
import json

import websockets
from pathlib import Path

from bluesky_pipeline.event_envelope import build_event_envelope

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
    event_count = 0

    collection_counts: dict[str, int] = {}
    operation_counts: dict[str, int] = {}

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with OUTPUT_PATH.open("a", encoding="utf-8") as output_file:
        async with websockets.connect(JETSTREAM_URL) as websocket:
            async for message in websocket:
                event = json.loads(message)

                envelope = build_event_envelope(event)

                output_file.write(json.dumps(envelope, ensure_ascii=False) + "\n")

                event_count += 1

                commit = event.get("commit", {})
                record = commit.get("record", {})
                subject = record.get("subject")

                if isinstance(subject, dict):
                    subject_uri = subject.get("uri")
                    subject_cid = subject.get("cid")
                else:
                    subject_uri = subject
                    subject_cid = None

                collection = commit.get("collection")
                collection_counts[collection] = collection_counts.get(collection, 0) + 1

                operation = commit.get("operation")
                operation_key = f"{collection}:{operation}"
                operation_counts[operation_key] = operation_counts.get(operation_key, 0) + 1


                # print(
                #     json.dumps(
                #         {
                #             "kind": event.get("kind"),
                #             "did": event.get("did"),
                #             "time_us": event.get("time_us"),
                #             "collection": commit.get("collection"),
                #             "operation": commit.get("operation"),
                #             "rkey": commit.get("rkey"),
                #             "record_type": record.get("$type"),
                #             "created_at": record.get("createdAt"),
                #             "subject_uri": subject_uri,
                #             "subject_cid": subject_cid,
                #         },
                #         indent=2,
                #         ensure_ascii=False,
                #     )
                # )

                if event_count >= 1000:
                    print("collection_counts: ", collection_counts)
                    print("operation_counts: ", operation_counts)
                    break

if __name__ == "__main__":
    asyncio.run(main())
