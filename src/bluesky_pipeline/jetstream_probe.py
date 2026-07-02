import asyncio
import json

import websockets

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

async def main() -> None:
    event_count = 0

    async with websockets.connect(JETSTREAM_URL) as websocket:
        async for message in websocket:
            event = json.loads(message)
            
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

            print(
                json.dumps(
                    {
                        "kind": event.get("kind"),
                        "did": event.get("did"),
                        "time_us": event.get("time_us"),
                        "collection": commit.get("collection"),
                        "operation": commit.get("operation"),
                        "rkey": commit.get("rkey"),
                        "record_type": record.get("$type"),
                        "created_at": record.get("createdAt"),
                        "subject_uri": subject_uri,
                        "subject_cid": subject_cid,
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )

            if event_count >= 20:
                break

if __name__ == "__main__":
    asyncio.run(main())
