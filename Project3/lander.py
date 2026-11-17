
"""Kafka topic 'bluesky' -> raw Parquet files in data/raw/.

A batch is written after 1000 records or 60 seconds.
Offsets are stored only AFTER the file exists.

Usage:
    python lander.py
    python lander.py crash
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import duckdb
from quixstreams import Application

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky"
OUT_DIR = "data/raw"
BATCH_SIZE = 1000
BATCH_SECONDS = 60
CRASH_AFTER_WRITE = "crash" in sys.argv[1:]

SCHEMA = """kafka_partition INTEGER, kafka_offset BIGINT, kafka_key VARCHAR,
    action VARCHAR, did VARCHAR, rkey VARCHAR, collection VARCHAR,
    operation VARCHAR, event_time VARCHAR, langs VARCHAR, is_reply BOOLEAN,
    landed_at TIMESTAMP, raw VARCHAR"""


def to_row(msg):
    """Keep the original JSON and safely extract Bluesky fields."""
    raw = msg.value().decode("utf8", errors="replace") if msg.value() else ""
    key = msg.key().decode("utf8", errors="replace") if msg.key() else None

    action = "malformed"
    did = rkey = collection = operation = event_time = langs = None
    is_reply = None

    try:
        message = json.loads(raw)
        if not isinstance(message, dict):
            raise ValueError("Expected a JSON object")

        event = message["data"]
        if not isinstance(event, dict):
            raise ValueError("Expected event object")

        action = message.get("action", "unknown")
        did = event.get("did")
        commit = event.get("commit") or {}

        rkey = commit.get("rkey")
        collection = commit.get("collection")
        operation = commit.get("operation")

        record = commit.get("record") or {}
        event_time = record.get("createdAt")

        languages = record.get("langs") or []
        langs = json.dumps(languages)

        if record:
            is_reply = bool(record.get("reply"))

    except (ValueError, KeyError, TypeError, AttributeError):
        action = "malformed"

    return (
        msg.partition(),
        msg.offset(),
        key,
        action,
        did,
        rkey,
        collection,
        operation,
        event_time,
        langs,
        is_reply,
        datetime.now(timezone.utc).replace(tzinfo=None),
        raw,
    )


def write_batch(rows):
    """Write Parquet to a temporary file, then rename atomically."""
    os.makedirs(OUT_DIR, exist_ok=True)

    name = (
        f"{OUT_DIR}/bluesky_"
        f"{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_"
        f"{uuid.uuid4().hex[:6]}.parquet"
    )

    con = duckdb.connect()
    con.execute(f"CREATE TABLE batch ({SCHEMA})")

    placeholders = ", ".join(["?"] * 13)
    con.executemany(
        f"INSERT INTO batch VALUES ({placeholders})",
        rows,
    )

    con.execute(f"COPY batch TO '{name}.tmp' (FORMAT parquet)")
    con.close()

    os.replace(f"{name}.tmp", name)
    return name


def main():
    app = Application(
        broker_address=BROKER,
        consumer_group="bluesky_lander",
        auto_offset_reset="earliest",
        loglevel="WARNING",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe([TOPIC])

        rows = []
        last = {}
        started = time.monotonic()

        print(
            "Landing records from",
            TOPIC,
            "into",
            OUT_DIR,
            "(Ctrl+C to stop)",
            flush=True,
        )

        while True:
            msg = consumer.poll(1)

            if msg is not None:
                if msg.error() is not None:
                    raise Exception(msg.error())

                if not rows:
                    started = time.monotonic()

                rows.append(to_row(msg))
                last[msg.partition()] = msg

            full = len(rows) >= BATCH_SIZE
            old = bool(rows) and (
                time.monotonic() - started >= BATCH_SECONDS
            )

            if full or old:
                path = write_batch(rows)
                print(
                    f"Landed {len(rows):4d} records -> {path}",
                    flush=True,
                )

                if CRASH_AFTER_WRITE:
                    print(
                        "CRASH (on purpose): file exists, "
                        "offsets were NOT stored",
                        flush=True,
                    )
                    os._exit(1)

                for message in last.values():
                    consumer.store_offsets(message=message)

                rows = []
                last = {}


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
