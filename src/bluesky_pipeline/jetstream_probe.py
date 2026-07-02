import asyncio
import json

import websockets

JETSTREAM_URL = (
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
)

async def main() -> None:
    event_count = 0

    async with websockets.connect(JETSTREAM_URL) as websocket:
        async for message in websocket:
            event = json.loads(message)
            if event.get("commit", {}).get("operation") != "create":
                continue
            
            event_count += 1

            commit - event.get("commit", {})
            record = commit.get("record", {})

            print(
                json.dumps(
                    {
                        "kind": event.get("kind"),
                        "did": event.get("did"),
                        "time_us": event.get("time_us"),
                        "collection": commit.get("collection"),
                        "operation": commit.get("operation"),
                        "rkey": commit.get("rkey"),
                        "created_at": record.get("createdAt"),
                        "text": record.get("text"),
                    },
                    indent=2,
                    ensure_ascii=False,
                )

            if event_count >= 1:
                break

if __name__ == "__main__":
    asyncio.run(main())
