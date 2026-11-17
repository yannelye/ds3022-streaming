
"""Bluesky Jetstream -> Kafka topic 'bluesky'.

Adapted from the class earthquake producer.
Streams live Bluesky events using a WebSocket.
"""
import asyncio
import json
import logging

from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky"
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe"


def send(producer, event):
    """Send one Bluesky event using DID as the Kafka key."""
    did = event.get("did")
    if not did:
        logging.warning("Skipped message without did")
        return False

    commit = event.get("commit") or {}
    action = commit.get("operation") or event.get("kind", "unknown")

    message = {
        "action": action,
        "data": event,
    }

    producer.produce(
        topic=TOPIC,
        key=did,
        value=json.dumps(message),
    )
    return True


async def live(producer):
    """Read Bluesky events and reconnect if disconnected."""
    total = 0

    while True:
        ws = None
        try:
            ws = await websocket_connect(
                WSS_URL,
                ping_interval=15,
                connect_timeout=30,
            )
            logging.info("Connected to Bluesky Jetstream")

            while True:
                text = await ws.read_message()

                if text is None:
                    logging.warning("Connection closed. Reconnecting...")
                    break

                try:
                    event = json.loads(text)

                    if not isinstance(event, dict):
                        logging.warning("Skipped non-object message")
                        continue

                    if send(producer, event):
                        total += 1

                        if total % 1000 == 0:
                            logging.info(
                                "Sent %d messages to %s",
                                total,
                                TOPIC,
                            )

                except (ValueError, TypeError, KeyError) as e:
                    logging.warning("Skipped invalid message: %s", e)

        except Exception as e:
            logging.warning("Connection error: %s", e)

        finally:
            if ws is not None:
                ws.close()

        logging.info("Retrying in 5 seconds...")
        await asyncio.sleep(5)


def main():
    app = Application(
        broker_address=BROKER,
        loglevel="WARNING",
    )

    with app.get_producer() as producer:
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        main()
    except KeyboardInterrupt:
        logging.info("Producer stopped")
