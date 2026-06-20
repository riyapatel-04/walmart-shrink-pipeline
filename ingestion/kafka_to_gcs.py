"""
Kafka → GCS Consumer
Reads from all 3 Kafka topics and lands raw JSON in GCS
partitioned by store_id / strategy / date
"""

import json
import os
from datetime import datetime, timezone
from google.cloud import storage
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GCS_BUCKET      = os.getenv("GCS_BUCKET", "walmart-shrink-raw-riya")
TOPICS          = ["pos.scan.events", "scale.weight.events", "pos.void.events"]

storage_client  = storage.Client()
bucket          = storage_client.bucket(GCS_BUCKET)


def gcs_path(topic: str, event: dict) -> str:
    store_id    = event.get("store_id", "UNKNOWN")
    strategy    = event.get("checkout_strategy", "UNKNOWN")
    date        = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    event_id    = event.get("event_id", "unknown")
    topic_clean = topic.replace(".", "_")
    return f"{topic_clean}/store_id={store_id}/strategy={strategy}/date={date}/{event_id}.json"


def upload_to_gcs(path: str, event: dict):
    blob = bucket.blob(path)
    blob.upload_from_string(
        data         = json.dumps(event),
        content_type = "application/json"
    )


def run_consumer():
    print(f"[INFO] Connecting to Kafka: {KAFKA_BOOTSTRAP}")
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers  = KAFKA_BOOTSTRAP,
        group_id           = "gcs-landing-consumer",
        auto_offset_reset  = "earliest",
        enable_auto_commit = True,
        value_deserializer = lambda m: json.loads(m.decode("utf-8")),
    )
    print(f"[INFO] Listening on: {TOPICS}")
    print(f"[INFO] Writing to GCS: gs://{GCS_BUCKET}")

    count = 0
    try:
        for message in consumer:
            event = message.value
            path  = gcs_path(message.topic, event)
            upload_to_gcs(path, event)
            count += 1
            if count % 50 == 0:
                print(f"[INFO] Uploaded {count} events. Last: {path}")
    except KeyboardInterrupt:
        print(f"\n[INFO] Stopped. Total uploaded: {count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()