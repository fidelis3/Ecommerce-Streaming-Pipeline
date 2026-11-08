E-commerce Streaming Pipeline

Kafka -> Snowflake -> dbt -> Airflow -> dashboard.

Quick start (Phases 1-2)
bash
docker compose up -d
python -m venv .venv && source .venv/bin/activate
pip install -r producer/requirements.txt
python producer/producer.py --rate 5
