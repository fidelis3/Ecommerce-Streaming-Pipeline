"""
Kafka -> Snowflake micro-batch loader.

Reads orders / payments / customers topics, buffers events, and flushes a
micro-batch to Snowflake RAW tables when EITHER:
  - the buffer reaches BATCH_SIZE messages, OR
  - BATCH_SECONDS have passed since the last flush.

Each batch is written to an NDJSON file, PUT to your Snowflake user stage,
then loaded with COPY INTO. Kafka offsets are committed only AFTER a
successful load (at-least-once), so RAW keeps topic/partition/offset.
"""
import json
import os
import time
import uuid
from collections import defaultdict
from pathlib import Path
import tempfile

from confluent_kafka import Consumer, KafkaError
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
BATCH_SECONDS = float(os.getenv("BATCH_SECONDS", "10"))

TOPIC_TO_TABLE = {
    "orders": "ORDERS_RAW",
    "payments": "PAYMENTS_RAW",
    "customers": "CUSTOMERS_RAW",
}


def snowflake_connection():
    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    with open(key_path, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    der = key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=der,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )


def load_batch(conn, topic, rows):
    """Write rows to NDJSON, PUT to the user stage, COPY INTO the RAW table."""
    table = TOPIC_TO_TABLE[topic]
    fname = f"{topic}_{uuid.uuid4().hex}.json"
    path = Path(tempfile.gettempdir()) / fname
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    cur = conn.cursor()
    try:
        cur.execute(
            f"PUT 'file://{path.as_posix()}' @~/ecom_loader/ "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        cur.execute(
            f"""
            COPY INTO {table} (record_content, kafka_topic, kafka_partition, kafka_offset)
            FROM (
                SELECT $1:record, $1:topic::STRING, $1:partition::NUMBER, $1:offset::NUMBER
                FROM @~/ecom_loader/{fname}
            )
            FILE_FORMAT = (TYPE = JSON)
            PURGE = TRUE
            """
        )
    finally:
        cur.close()
        path.unlink(missing_ok=True)


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "snowflake-loader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(list(TOPIC_TO_TABLE))
    conn = snowflake_connection()
    print(f"connected. batch_size={BATCH_SIZE} batch_seconds={BATCH_SECONDS}. Ctrl+C to stop.")

    buffer = defaultdict(list)
    buffered = 0
    last_flush = time.time()
    total = 0

    def flush():
        nonlocal buffered, last_flush, total
        if not buffered:
            last_flush = time.time()
            return
        for topic, rows in buffer.items():
            load_batch(conn, topic, rows)
        consumer.commit(asynchronous=False)
        total += buffered
        print(f"loaded {buffered} events (total {total})")
        buffer.clear()
        buffered = 0
        last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"kafka error: {msg.error()}")
                else:
                    try:
                        record = json.loads(msg.value())
                    except (json.JSONDecodeError, TypeError):
                        record = {"_unparseable": msg.value().decode("utf-8", "replace")}
                    buffer[msg.topic()].append({
                        "record": record,
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    })
                    buffered += 1

            if buffered >= BATCH_SIZE or (time.time() - last_flush >= BATCH_SECONDS):
                flush()
    except KeyboardInterrupt:
        print("stopping...")
    finally:
        flush()
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()