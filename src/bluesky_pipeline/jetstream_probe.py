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
            event_count += 1

            print(
                {
                    "kind": event.get("kind"),
                    "did": event.get("did"),
                    "time_us": event.get("time_us"),
                    "collection": event.get("commit", {}).get("collection"),
                    "operation": event.get("commit", {}).get("operation"),
                }
            )

            if event_count >= 5:
                break

if __name__ == "__main__":
    asyncio.run(main())
