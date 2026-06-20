"""
─────────────────────────────────────────────────────────────────────
Walmart Self-Checkout Shrink Intelligence Platform
Event Simulator — simulate_store.py

Simulates realistic POS scan, weight sensor, and void events across
multiple Walmart stores with configurable fraud rate and checkout
strategy mix. Streams events directly into Kafka.

Topics produced:
  • pos.scan.events      — item scanned at checkout lane
  • scale.weight.events  — bagging area weight reading
  • pos.void.events      — void or override action

Usage:
  py simulate_store.py --dry_run --stores 2 --lanes 2 --max_sessions 5
  py simulate_store.py --stores 5 --lanes 4 --fraud_rate 0.035
─────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("[WARN] kafka-python not installed. Run: pip install kafka-python")


# ─────────────────────────────────────────────────────────────────
#  PRODUCT CATALOGUE — real product names, weights, prices
# ─────────────────────────────────────────────────────────────────

PRODUCTS = [
    {"upc": "0000000000001", "name": "Bananas (bunch)",        "weight_g": 680,  "price": 1.49},
    {"upc": "0000000000002", "name": "Whole Milk (1 gal)",     "weight_g": 3856, "price": 3.98},
    {"upc": "0000000000003", "name": "White Bread",            "weight_g": 567,  "price": 2.47},
    {"upc": "0000000000004", "name": "Eggs (12ct)",            "weight_g": 680,  "price": 4.97},
    {"upc": "0000000000005", "name": "Chicken Breast (2lb)",   "weight_g": 907,  "price": 8.96},
    {"upc": "0000000000006", "name": "Cheddar Cheese (8oz)",   "weight_g": 227,  "price": 3.97},
    {"upc": "0000000000007", "name": "Orange Juice (52oz)",    "weight_g": 1560, "price": 4.47},
    {"upc": "0000000000008", "name": "Pasta (16oz)",           "weight_g": 454,  "price": 1.24},
    {"upc": "0000000000009", "name": "Tomato Sauce (24oz)",    "weight_g": 680,  "price": 1.48},
    {"upc": "0000000000010", "name": "Greek Yogurt (32oz)",    "weight_g": 907,  "price": 5.97},
    {"upc": "0000000000011", "name": "Ribeye Steak (1lb)",     "weight_g": 454,  "price": 14.96},
    {"upc": "0000000000012", "name": "Salmon Fillet (1lb)",    "weight_g": 454,  "price": 9.97},
    {"upc": "0000000000013", "name": "Laundry Detergent",      "weight_g": 2722, "price": 11.97},
    {"upc": "0000000000014", "name": "Paper Towels (6pk)",     "weight_g": 1134, "price": 8.97},
    {"upc": "0000000000015", "name": "Shampoo (12oz)",         "weight_g": 354,  "price": 5.47},
    {"upc": "0000000000016", "name": "Toothpaste (6oz)",       "weight_g": 170,  "price": 3.47},
    {"upc": "0000000000017", "name": "Frozen Pizza",           "weight_g": 794,  "price": 5.97},
    {"upc": "0000000000018", "name": "Ice Cream (1.5qt)",      "weight_g": 850,  "price": 4.97},
    {"upc": "0000000000019", "name": "Coffee (12oz)",          "weight_g": 340,  "price": 8.97},
    {"upc": "0000000000020", "name": "Sparkling Water (12pk)", "weight_g": 4536, "price": 5.97},
]

CHECKOUT_STRATEGIES = ["SCO", "STAFFED", "AI_SCAN", "SCO_RESTRICTED"]

FRAUD_RATES = {
    "SCO":            0.035,
    "STAFFED":        0.002,
    "AI_SCAN":        0.010,
    "SCO_RESTRICTED": 0.015,
}

VOID_REASONS_CLEAN = ["PRICE_OVERRIDE", "ITEM_NOT_FOUND", "CUSTOMER_CHANGED_MIND", "DUPLICATE_SCAN"]
VOID_REASONS_MESSY = ["price_override", "NOT_FOUND", "changed mind", "DUPE", ""]

STORE_IDS = [f"WMT_{str(i).zfill(3)}" for i in range(1, 101)]


# ─────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def jitter_ts(base_ts, lag_ms):
    from datetime import timedelta
    dt = datetime.fromisoformat(base_ts)
    return (dt + timedelta(milliseconds=lag_ms)).isoformat()

def make_event_id():
    return str(uuid.uuid4())

def corrupt_price(price):
    if random.random() < 0.01:
        return f"${price:.2f}"
    return round(price, 2)

def corrupt_void_reason(reason):
    if random.random() < 0.02:
        return random.choice(VOID_REASONS_MESSY)
    return reason


# ─────────────────────────────────────────────────────────────────
#  EVENT GENERATORS
# ─────────────────────────────────────────────────────────────────

def generate_scan_event(store_id, lane_id, strategy, session_id, product):
    ts = now_iso()
    return {
        "event_id":          make_event_id(),
        "session_id":        session_id,
        "store_id":          store_id,
        "lane_id":           lane_id,
        "checkout_strategy": strategy,
        "item_upc":          product["upc"],
        "item_name":         product["name"],
        "price":             corrupt_price(product["price"]),
        "event_type":        "SCAN",
        "event_ts":          ts,
        "ingested_at":       ts,
    }

def generate_weight_event(store_id, lane_id, strategy, session_id, product, is_fraud=False):
    if random.random() < 0.05:
        return None

    scan_ts = now_iso()
    lag_ms  = random.randint(200, 800)

    if is_fraud:
        extra_items  = random.randint(1, 3)
        extra_weight = sum(random.choice(PRODUCTS)["weight_g"] for _ in range(extra_items))
        actual_weight = product["weight_g"] + extra_weight
    else:
        variance      = random.uniform(-0.03, 0.03)
        actual_weight = int(product["weight_g"] * (1 + variance))

    if random.random() < 0.005:
        actual_weight = -abs(actual_weight)

    variance_pct = round(
        abs(actual_weight - product["weight_g"]) / product["weight_g"] * 100, 2
    ) if product["weight_g"] > 0 else 0.0

    return {
        "event_id":          make_event_id(),
        "session_id":        session_id,
        "store_id":          store_id,
        "lane_id":           lane_id,
        "checkout_strategy": strategy,
        "expected_weight_g": product["weight_g"],
        "actual_weight_g":   actual_weight,
        "variance_pct":      variance_pct,
        "weight_match":      variance_pct < 10.0,
        "event_ts":          jitter_ts(scan_ts, lag_ms),
        "ingested_at":       now_iso(),
    }

def generate_void_event(store_id, lane_id, strategy, session_id, voided_amount, is_suspicious=False):
    reason = random.choice(
        ["PRICE_OVERRIDE", "DUPLICATE_SCAN"] if is_suspicious
        else VOID_REASONS_CLEAN
    )
    return {
        "event_id":          make_event_id(),
        "session_id":        session_id,
        "store_id":          store_id,
        "lane_id":           lane_id,
        "checkout_strategy": strategy,
        "void_reason":       corrupt_void_reason(reason),
        "voided_amount":     round(voided_amount, 2),
        "approved_by":       random.choice(["ATTENDANT", "SELF", None]),
        "event_ts":          now_iso(),
        "ingested_at":       now_iso(),
    }


# ─────────────────────────────────────────────────────────────────
#  SESSION SIMULATOR
# ─────────────────────────────────────────────────────────────────

def simulate_checkout_session(store_id, lane_id, strategy):
    session_id = make_event_id()
    is_fraud   = random.random() < FRAUD_RATES[strategy]
    num_items  = random.randint(2, 12)
    basket     = random.choices(PRODUCTS, k=num_items)

    scan_events   = []
    weight_events = []
    void_events   = []

    for product in basket:
        scan = generate_scan_event(store_id, lane_id, strategy, session_id, product)
        is_dup = random.random() < 0.02
        scan_events.append((scan, is_dup))

        weight = generate_weight_event(store_id, lane_id, strategy, session_id, product, is_fraud=is_fraud)
        if weight:
            weight_events.append(weight)

    num_voids = (
        random.randint(1, 3) if is_fraud
        else random.choices([0, 1], weights=[0.85, 0.15])[0]
    )
    for _ in range(num_voids):
        voided_product = random.choice(basket)
        void_events.append(generate_void_event(
            store_id, lane_id, strategy, session_id,
            voided_amount=voided_product["price"],
            is_suspicious=is_fraud
        ))

    return {
        "session_id":    session_id,
        "store_id":      store_id,
        "lane_id":       lane_id,
        "strategy":      strategy,
        "is_fraud":      is_fraud,
        "scan_events":   scan_events,
        "weight_events": weight_events,
        "void_events":   void_events,
    }


# ─────────────────────────────────────────────────────────────────
#  KAFKA PRODUCER
# ─────────────────────────────────────────────────────────────────

def make_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )

def send_event(producer, topic, key, event, dry_run=False):
    if dry_run:
        print(f"\n[{topic}] key={key}")
        print(json.dumps(event, indent=2))
        return
    producer.send(topic, key=key, value=event)


# ─────────────────────────────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────────────────────────────

def parse_strategy_mix(mix_str):
    result = {}
    for part in mix_str.split(","):
        strategy, weight = part.strip().split(":")
        result[strategy.strip()] = float(weight.strip())
    return result


def run_simulator(stores, lanes, strategy_mix, kafka_bootstrap, sessions_per_second, dry_run, max_sessions):
    store_ids  = random.sample(STORE_IDS, min(stores, len(STORE_IDS)))
    strategies = list(strategy_mix.keys())
    weights    = list(strategy_mix.values())

    store_strategies = {
        store_id: random.choices(strategies, weights=weights)[0]
        for store_id in store_ids
    }

    producer = None
    if not dry_run:
        if not KAFKA_AVAILABLE:
            print("[ERROR] kafka-python not installed. Run: pip install kafka-python")
            return
        print(f"[INFO] Connecting to Kafka at {kafka_bootstrap}...")
        producer = make_producer(kafka_bootstrap)
        print("[INFO] Kafka producer ready.")

    print("\n" + "─" * 60)
    print("  Walmart Self-Checkout Shrink Simulator")
    print("─" * 60)
    print(f"  Stores:           {stores}")
    print(f"  Lanes per store:  {lanes}")
    print(f"  Strategy mix:     {strategy_mix}")
    print(f"  Sessions/sec:     {sessions_per_second}")
    print(f"  Dry run:          {dry_run}")
    print(f"  Max sessions:     {max_sessions or 'unlimited'}")
    print("─" * 60 + "\n")

    session_count = 0
    fraud_count   = 0
    interval      = 1.0 / sessions_per_second

    try:
        while True:
            store_id = random.choice(store_ids)
            lane_id  = f"LANE_{str(random.randint(1, lanes)).zfill(2)}"
            strategy = store_strategies[store_id]

            session = simulate_checkout_session(store_id, lane_id, strategy)
            session_count += 1
            if session["is_fraud"]:
                fraud_count += 1

            for scan, is_dup in session["scan_events"]:
                send_event(producer, "pos.scan.events", f"{store_id}:{lane_id}", scan, dry_run)
                if is_dup:
                    send_event(producer, "pos.scan.events", f"{store_id}:{lane_id}", scan, dry_run)

            for weight in session["weight_events"]:
                send_event(producer, "scale.weight.events", f"{store_id}:{lane_id}", weight, dry_run)

            for void in session["void_events"]:
                send_event(producer, "pos.void.events", f"{store_id}:{lane_id}", void, dry_run)

            if producer and session_count % 100 == 0:
                producer.flush()

            if session_count % 50 == 0:
                fraud_rate_actual = fraud_count / session_count * 100
                print(f"[{now_iso()}] Sessions: {session_count:,} | Fraud: {fraud_count:,} ({fraud_rate_actual:.1f}%) | Store: {store_id} | Strategy: {strategy}")

            if max_sessions and session_count >= max_sessions:
                print(f"\n[INFO] Reached max_sessions={max_sessions}. Stopping.")
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n[INFO] Simulator stopped.")
    finally:
        if producer:
            producer.flush()
            producer.close()
        print("\n" + "─" * 60)
        print(f"  Total sessions:  {session_count:,}")
        print(f"  Fraud sessions:  {fraud_count:,} ({fraud_count/max(session_count,1)*100:.1f}%)")
        print("─" * 60)


# ─────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Walmart Self-Checkout Shrink Event Simulator")
    parser.add_argument("--stores",           type=int,   default=10)
    parser.add_argument("--lanes",            type=int,   default=6)
    parser.add_argument("--strategy_mix",     type=str,   default="SCO:0.4,STAFFED:0.3,AI_SCAN:0.2,SCO_RESTRICTED:0.1")
    parser.add_argument("--kafka",            type=str,   default="localhost:9092")
    parser.add_argument("--sessions_per_sec", type=float, default=2.0)
    parser.add_argument("--max_sessions",     type=int,   default=None)
    parser.add_argument("--dry_run",          action="store_true")
    args = parser.parse_args()

    strategy_mix = parse_strategy_mix(args.strategy_mix)

    run_simulator(
        stores              = args.stores,
        lanes               = args.lanes,
        strategy_mix        = strategy_mix,
        kafka_bootstrap     = args.kafka,
        sessions_per_second = args.sessions_per_sec,
        dry_run             = args.dry_run,
        max_sessions        = args.max_sessions,
    )

if __name__ == "__main__":
    main()