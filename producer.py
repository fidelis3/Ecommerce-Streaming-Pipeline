"""
Fake e-commerce event producer.

Emits three event types to Kafka:
  - orders    : order_placed, order_shipped, order_cancelled
  - payments  : payment_succeeded, payment_failed
  - customers : customer_created, customer_updated (feeds the SCD2 snapshot later)

Deliberately messy: duplicates, null fields, and late-arriving events,
so your dbt tests and dedup logic have real problems to solve.

Usage:
    python producer.py --rate 5            # ~5 events/sec, runs until Ctrl+C
    python producer.py --rate 20 --count 500
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

BOOTSTRAP = "localhost:9092"
TOPICS = ["orders", "payments", "customers"]

DUPLICATE_RATE = 0.03   # 3% of events are sent twice
NULL_RATE = 0.02        # 2% have a required field nulled out
LATE_RATE = 0.05        # 5% arrive with a timestamp up to 2 hours old

fake = Faker()
PRODUCTS = [
    ("P001", "Wireless Mouse", 24.99),
    ("P002", "Mechanical Keyboard", 89.00),
    ("P003", "USB-C Hub", 39.50),
    ("P004", "27in Monitor", 249.00),
    ("P005", "Laptop Stand", 34.99),
    ("P006", "Webcam 1080p", 59.95),
]


def ensure_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    new = [NewTopic(t, num_partitions=3, replication_factor=1) for t in TOPICS if t not in existing]
    if new:
        for topic, fut in admin.create_topics(new).items():
            fut.result()
            print(f"created topic: {topic}")


def now_ts(late=False):
    ts = datetime.now(timezone.utc)
    if late:
        ts -= timedelta(minutes=random.randint(5, 120))
    return ts.isoformat()


def maybe_null(event, field):
    if random.random() < NULL_RATE:
        event[field] = None
    return event


def make_customer():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "customer_created",
        "customer_id": f"C{random.randint(1, 500):04d}",
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "event_ts": now_ts(),
    }


def make_order(customers):
    cust = random.choice(customers)
    prod_id, prod_name, price = random.choice(PRODUCTS)
    qty = random.randint(1, 4)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_placed",
        "order_id": f"O{uuid.uuid4().hex[:10]}",
        "customer_id": cust["customer_id"],
        "product_id": prod_id,
        "product_name": prod_name,
        "quantity": qty,
        "unit_price": price,
        "event_ts": now_ts(late=random.random() < LATE_RATE),
    }


def make_status_update(order):
    status = random.choices(["order_shipped", "order_cancelled"], weights=[85, 15])[0]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": status,
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "event_ts": now_ts(late=random.random() < LATE_RATE),
    }


def make_payment(order):
    ok = random.random() > 0.08
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "payment_succeeded" if ok else "payment_failed",
        "payment_id": f"PAY{uuid.uuid4().hex[:10]}",
        "order_id": order["order_id"],
        "amount": round(order["quantity"] * order["unit_price"], 2),
        "method": random.choice(["card", "paypal", "mpesa", "bank_transfer"]),
        "event_ts": now_ts(late=random.random() < LATE_RATE),
    }



def send(producer, topic, key, event):
    """Send an event, occasionally twice (simulates at-least-once delivery)."""
    payload = json.dumps(event).encode()
    producer.produce(topic, key=key.encode(), value=payload)
    if random.random() < DUPLICATE_RATE:
        producer.produce(topic, key=key.encode(), value=payload)
    producer.poll(0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=5, help="events per second")
    parser.add_argument("--count", type=int, default=0, help="stop after N orders (0 = forever)")
    args = parser.parse_args()

    ensure_topics()
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": False})

    # Seed some customers first
    customers = [make_customer() for _ in range(50)]
    for c in customers:
        c["customer_id"] = f"C{customers.index(c) + 1:04d}"
        send(producer, "customers", c["customer_id"], c)

    open_orders = []
    sent = 0
    print(f"producing at ~{args.rate} orders/sec. Ctrl+C to stop.")
    try:
        while args.count == 0 or sent < args.count:
            order = make_order(customers)
            maybe_null(order, "customer_id")
            send(producer, "orders", order["order_id"], order)
            send(producer, "payments", order["order_id"], maybe_null(make_payment(order), "amount"))
            open_orders.append(order)
            sent += 1

            # Some earlier orders get status updates
            if open_orders and random.random() < 0.6:
                o = open_orders.pop(random.randrange(len(open_orders)))
                send(producer, "orders", o["order_id"], make_status_update(o))

            # Occasionally a customer changes city/email (for SCD2 later)
            if random.random() < 0.05:
                c = random.choice(customers)
                c.update(event_id=str(uuid.uuid4()), event_type="customer_updated",
                         city=fake.city(), email=fake.email(), event_ts=now_ts())
                send(producer, "customers", c["customer_id"], c)

            if sent % 50 == 0:
                print(f"{sent} orders sent")
            time.sleep(1 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"done. {sent} orders sent.")


if __name__ == "__main__":
    main()