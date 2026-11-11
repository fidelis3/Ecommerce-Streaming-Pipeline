# E-commerce Streaming Pipeline

Real-time e-commerce data pipeline: **Kafka → Snowflake → dbt → Airflow**, with a star-schema warehouse and data quality tests.

A Python producer simulates an online store's event stream (orders, payments, customer changes) and publishes it to Apache Kafka. A micro-batch consumer lands the events in Snowflake's raw layer, where dbt will transform them into analytics-ready tables. The data is intentionally messy (duplicates, nulls, late-arriving events) so the downstream cleaning, testing, and deduplication logic has real problems to solve.

## Architecture

```
Python producer (Faker)
        │  JSON events
        ▼
Kafka (KRaft, Docker) ── topics: orders · payments · customers
        │
        ▼
Python micro-batch consumer ── PUT to stage + COPY INTO
        │
        ▼
Snowflake  ECOM.RAW  (VARIANT + topic/partition/offset)
        │
        ▼
dbt:  staging → intermediate → marts  
        │
        ▼
Dashboard (Metabase / Streamlit)  

Airflow orchestrates dbt runs and freshness checks
```

## Status

- [x] **Phase 1:** Kafka in KRaft mode via Docker Compose (no ZooKeeper)
- [x] **Phase 2:** Event producer with deliberate data-quality problems
- [ ] **Phase 3:** Snowflake RAW layer + micro-batch consumer *(in progress)*
- [ ] **Phase 4:** dbt models, tests, snapshots, docs
- [ ] **Phase 5:** Airflow orchestration
- [ ] **Phase 6:** Dashboard

## Tech stack

| Tool | Role |
|---|---|
| Apache Kafka 3.8 (KRaft) | Event streaming backbone |
| Docker Compose | Runs Kafka locally |
| Python (`confluent-kafka`, `Faker`) | Event producer and consumer |
| Snowflake | Cloud data warehouse |
| dbt | SQL transformations and tests *(planned)* |
| Apache Airflow | Orchestration *(planned)* |

## Project structure

```
.
├── docker-compose.yml     # Kafka broker + controller (KRaft)
├── producer.py            # Generates fake e-commerce events
├── consumer.py            # Kafka → Snowflake micro-batch loader
├── requirements.txt
├── .env.example           # Config template (copy to .env)
└── README.md
```

## Events

| Topic | Event types | Key |
|---|---|---|
| `orders` | `order_placed`, `order_shipped`, `order_cancelled` | `order_id` |
| `payments` | `payment_succeeded`, `payment_failed` | `order_id` |
| `customers` | `customer_created`, `customer_updated` | `customer_id` |

Each topic has 3 partitions. Messages are keyed so all events for one entity land in the same partition, which preserves their order.

**Deliberate mess in the data:**
- ~3% of events are sent twice (simulates at-least-once delivery)
- ~2% have a required field set to `null`
- ~5% arrive late, with timestamps up to 2 hours old
- `customer_updated` events change city/email, to feed an SCD Type 2 snapshot later

## Quick start

**Prerequisites:** Docker Desktop (running), Python 3.10–3.12, a Snowflake account.

### 1. Start Kafka and the producer

```bash
docker compose up -d
python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows PowerShell (macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
python producer.py --rate 5         # ~5 orders/sec, Ctrl+C to stop
```

Verify events are in Kafka:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic orders
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning --max-messages 5
```

### 2. Set up Snowflake

Create the `ECOM` database with `RAW`, `STAGING`, and `ANALYTICS` schemas, an X-Small warehouse (`ECOM_WH`, 60-second auto-suspend), a resource monitor, an `ECOM_ROLE`, a service user `ECOM_USER`, and the three RAW tables (`ORDERS_RAW`, `PAYMENTS_RAW`, `CUSTOMERS_RAW`).

Authentication uses **key-pair auth**: generate an RSA key pair, keep the private key (`rsa_key.p8`) local, and attach the public key to the user with `ALTER USER ECOM_USER SET RSA_PUBLIC_KEY='...'`.

### 3. Configure and run the consumer

```bash
cp .env.example .env     # then fill in your account identifier and key path
pip install snowflake-connector-python cryptography python-dotenv
python consumer.py
```

Check the load in Snowflake:

```sql
SELECT record_content:event_type::STRING AS event_type, kafka_partition, kafka_offset
FROM ECOM.RAW.ORDERS_RAW LIMIT 10;
```

## Design decisions and trade-offs

- **KRaft instead of ZooKeeper.** Kafka's built-in Raft consensus protocol manages cluster metadata, so there is one less system to run. The single node acts as both broker and controller, which is fine for local development; production would use several brokers and a dedicated controller quorum.
- **ELT, not ETL.** Raw JSON lands untouched in a `VARIANT` column, and transformation happens afterwards inside Snowflake with dbt. This keeps the raw layer replayable and moves the heavy compute to the warehouse.
- **Micro-batch loading.** The consumer flushes when a batch hits `BATCH_SIZE` events or `BATCH_SECONDS` have passed, whichever comes first. Larger batches mean fewer, cheaper loads but higher latency; smaller ones mean fresher data but more load overhead. Both are configurable in `.env`.
- **Stage + `COPY INTO`, not row-by-row inserts.** Each batch is written to a file, uploaded to a Snowflake stage, and bulk-loaded. This is much faster and cheaper than individual `INSERT`s.
- **At-least-once delivery.** Kafka offsets are committed only after a batch loads successfully. A crash can cause duplicates, which is why RAW keeps `kafka_topic`, `kafka_partition`, and `kafka_offset` so dbt can deduplicate and trace every row.
- **Cost controls.** X-Small warehouse, 60-second auto-suspend, and a resource monitor that suspends the warehouse at its credit quota.
- **Secrets stay out of git.** Credentials live in `.env` and the private key in a `.p8` file, both git-ignored.

## Planned: dbt layers

- **Staging:** parse the JSON, rename and cast columns, deduplicate on `event_id`.
- **Intermediate:** join orders with payments and customers.
- **Marts:** star schema with `fct_orders`, `dim_customers`, `dim_products`, plus a daily revenue table.
- Incremental model for the fact table, tests (`unique`, `not_null`, `relationships`, `accepted_values`), an SCD Type 2 snapshot for customers, and `dbt docs` for lineage.

## Stretch goals

- Kafka Streams or ksqlDB to filter/enrich events before loading
- Slack alerts from Airflow on failed runs or stale data
- CI with GitHub Actions running `dbt build` on pull requests
- Swap the Python consumer for the Snowflake Kafka Connector and compare

## Cleanup

```bash
docker compose down -v      # stop Kafka and delete its data
```

Remember Snowflake warehouses auto-suspend, but you can also run `ALTER WAREHOUSE ECOM_WH SUSPEND;` to be sure.