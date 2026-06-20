"""
Scored Sessions → BigQuery Writer
Reads from pos.scored.sessions Kafka topic
Writes each scored session to BigQuery scored_sessions table
"""

import json
import os
from kafka import KafkaConsumer
from google.cloud import bigquery

KAFKA_BOOTSTRAP = "localhost:9092"
PROJECT_ID      = "walmart-shrink-riya-2026"
DATASET         = "walmart_shrink"
TABLE           = "scored_sessions"
TABLE_ID        = f"{PROJECT_ID}.{DATASET}.{TABLE}"

client = bigquery.Client(project=PROJECT_ID)


def parse_row(event: dict) -> dict:
    """Clean and type-cast scored session for BigQuery."""
    return {
        "scored_session_id":  str(event.get("scored_session_id", "")),
        "session_id":         str(event.get("session_id", "")),
        "store_id":           str(event.get("store_id", "")),
        "lane_id":            str(event.get("lane_id", "")),
        "checkout_strategy":  str(event.get("checkout_strategy", "")),
        "scored_at":          event.get("scored_at"),
        "started_at":         event.get("started_at"),
        "fraud_score":        int(event.get("fraud_score", 0)),
        "fraud_flag":         bool(event.get("fraud_flag", False)),
        "scanned_items":      int(event.get("scanned_items", 0)),
        "weighed_items":      int(event.get("weighed_items", 0)),
        "void_count":         int(event.get("void_count", 0)),
        "estimated_loss_usd": float(event.get("estimated_loss_usd", 0.0)),
    }


def run_writer():
    print(f"[INFO] Connecting to Kafka: {KAFKA_BOOTSTRAP}")
    print(f"[INFO] Writing to BigQuery: {TABLE_ID}")

    consumer = KafkaConsumer(
        "pos.scored.sessions",
        bootstrap_servers    = KAFKA_BOOTSTRAP,
        group_id             = "bigquery-writer",
        auto_offset_reset    = "earliest",
        enable_auto_commit   = True,
        value_deserializer   = lambda m: json.loads(m.decode("utf-8")),
    )

    batch      = []
    batch_size = 10
    count      = 0

    try:
        for message in consumer:
            event = message.value
            row   = parse_row(event)
            batch.append(row)

            if len(batch) >= batch_size:
                errors = client.insert_rows_json(TABLE_ID, batch)
                if errors:
                    print(f"[ERROR] BigQuery insert errors: {errors}")
                else:
                    count += len(batch)
                    print(f"[INFO] Inserted {count} rows into BigQuery")
                batch = []

    except KeyboardInterrupt:
        if batch:
            errors = client.insert_rows_json(TABLE_ID, batch)
            if not errors:
                count += len(batch)
        print(f"\n[INFO] Stopped. Total rows inserted: {count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_writer()