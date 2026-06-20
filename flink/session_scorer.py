"""
─────────────────────────────────────────────────────────────────────
Walmart Self-Checkout Shrink Intelligence Platform
Flink Session Scorer — session_scorer.py

Reads from 3 Kafka topics:
  - pos.scan.events
  - scale.weight.events
  - pos.void.events

Joins events within a 90-second session window keyed by
store_id + lane_id. Computes fraud score 0-100 per session.

Outputs scored sessions to:
  - pos.scored.sessions (Kafka topic)
  - BigQuery (via GCS staging)

Fraud signals detected:
  1. Weight mismatch   — actual weight >> expected weight
  2. Scan skip         — more weight than scanned items
  3. Void abuse        — more than 2 voids in one session
  4. High value items  — expensive items with weight mismatch
─────────────────────────────────────────────────────────────────────
"""

import json
import os
import uuid
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict
import threading
import time


# ─────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SESSION_GAP_SECS    = 10      # session closes after 90s of inactivity
FRAUD_THRESHOLD     = 65      # score above this triggers LP alert
WEIGHT_MISMATCH_PCT = 15.0    # weight variance above this = mismatch signal


# ─────────────────────────────────────────────────────────────────
#  IN-MEMORY SESSION STORE
#  In production this would be Flink state backend (RocksDB)
#  For portfolio: Python dict with TTL cleanup
# ─────────────────────────────────────────────────────────────────

class SessionStore:
    """
    Holds all events for active checkout sessions.
    Key: store_id:lane_id
    Value: dict of session events + metadata
    """
    def __init__(self):
        self.sessions = defaultdict(lambda: {
            "session_id":        None,
            "store_id":          None,
            "lane_id":           None,
            "checkout_strategy": None,
            "scan_events":       [],
            "weight_events":     [],
            "void_events":       [],
            "last_event_ts":     None,
            "started_at":        datetime.now(timezone.utc).isoformat(),
        })
        self.lock = threading.Lock()

    def add_event(self, lane_key: str, topic: str, event: dict):
        with self.lock:
            session = self.sessions[lane_key]

            # Set session metadata from first event
            if session["session_id"] is None:
                session["session_id"]        = event.get("session_id", str(uuid.uuid4()))
                session["store_id"]          = event.get("store_id")
                session["lane_id"]           = event.get("lane_id")
                session["checkout_strategy"] = event.get("checkout_strategy")

            # Route event to correct bucket
            if topic == "pos.scan.events":
                session["scan_events"].append(event)
            elif topic == "scale.weight.events":
                session["weight_events"].append(event)
            elif topic == "pos.void.events":
                session["void_events"].append(event)

            session["last_event_ts"] = time.time()

    def get_expired_sessions(self) -> list:
        """Return sessions that have been inactive for SESSION_GAP_SECS."""
        expired = []
        now = time.time()
        with self.lock:
            for lane_key, session in list(self.sessions.items()):
                last_ts = session["last_event_ts"]
                if last_ts and (now - last_ts) > SESSION_GAP_SECS:
                    expired.append((lane_key, session.copy()))
        return expired

    def remove_session(self, lane_key: str):
        with self.lock:
            if lane_key in self.sessions:
                del self.sessions[lane_key]


# ─────────────────────────────────────────────────────────────────
#  FRAUD SCORER
# ─────────────────────────────────────────────────────────────────

def compute_fraud_score(session: dict) -> dict:
    """
    Compute fraud score 0-100 for a completed session.
    Returns score + breakdown of which signals fired.
    """
    score           = 0
    signals_fired   = []
    scan_events     = session["scan_events"]
    weight_events   = session["weight_events"]
    void_events     = session["void_events"]

    # ── Signal 1: Weight mismatch ─────────────────────────────
    # Weight sensor detected more than scanned item weight
    mismatches = [w for w in weight_events if not w.get("weight_match", True)]
    if mismatches:
        mismatch_score = min(40, len(mismatches) * 15)
        score += mismatch_score
        signals_fired.append({
            "signal":    "WEIGHT_MISMATCH",
            "count":     len(mismatches),
            "score_add": mismatch_score,
            "detail":    f"{len(mismatches)} items with weight variance > {WEIGHT_MISMATCH_PCT}%"
        })

    # ── Signal 2: Scan count vs weight event count mismatch ───
    # More weight events than scan events = unscanned items
    scanned_count = len(scan_events)
    weighed_count = len(weight_events)
    if weighed_count > scanned_count:
        skip_count  = weighed_count - scanned_count
        skip_score  = min(30, skip_count * 10)
        score      += skip_score
        signals_fired.append({
            "signal":    "SCAN_SKIP",
            "count":     skip_count,
            "score_add": skip_score,
            "detail":    f"{skip_count} items weighed but not scanned"
        })

    # ── Signal 3: Void abuse ──────────────────────────────────
    # More than 2 voids in one session is suspicious
    void_count = len(void_events)
    if void_count > 2:
        void_score  = min(20, (void_count - 2) * 10)
        score      += void_score
        signals_fired.append({
            "signal":    "VOID_ABUSE",
            "count":     void_count,
            "score_add": void_score,
            "detail":    f"{void_count} void events in one session"
        })

    # ── Signal 4: High value items with weight mismatch ───────
    # Expensive items (>$10) with weight mismatch = higher risk
    high_value_scans = [s for s in scan_events if isinstance(s.get("price"), (int, float)) and s.get("price", 0) > 10.0]
    if high_value_scans and mismatches:
        score += 10
        signals_fired.append({
            "signal":    "HIGH_VALUE_MISMATCH",
            "count":     len(high_value_scans),
            "score_add": 10,
            "detail":    f"{len(high_value_scans)} high-value items with weight mismatch"
        })

    # Cap at 100
    score = min(100, score)

    # Estimate loss amount
    estimated_loss = sum(
        s.get("price", 0) for s in scan_events
        if isinstance(s.get("price"), (int, float))
    ) * 0.035 if score > FRAUD_THRESHOLD else 0.0

    return {
        "fraud_score":      score,
        "fraud_flag":       score > FRAUD_THRESHOLD,
        "signals_fired":    signals_fired,
        "scanned_items":    scanned_count,
        "weighed_items":    weighed_count,
        "void_count":       void_count,
        "estimated_loss_usd": round(estimated_loss, 2),
    }


# ─────────────────────────────────────────────────────────────────
#  SESSION CLOSER
#  Runs in background thread, checks for expired sessions every 5s
# ─────────────────────────────────────────────────────────────────

def session_closer(store: SessionStore, producer: KafkaProducer, results: list):
    """
    Background thread that closes expired sessions,
    scores them, and sends to pos.scored.sessions topic.
    """
    while True:
        time.sleep(5)  # check every 5 seconds
        expired = store.get_expired_sessions()

        for lane_key, session in expired:
            score_result = compute_fraud_score(session)

            scored_session = {
                "scored_session_id":  str(uuid.uuid4()),
                "session_id":         session["session_id"],
                "store_id":           session["store_id"],
                "lane_id":            session["lane_id"],
                "checkout_strategy":  session["checkout_strategy"],
                "scored_at":          datetime.now(timezone.utc).isoformat(),
                "started_at":         session["started_at"],
                **score_result,
            }

            # Send to Kafka output topic
            if producer:
                producer.send(
                    "pos.scored.sessions",
                    key=lane_key.encode("utf-8"),
                    value=json.dumps(scored_session).encode("utf-8")
                )

            # Store in results list for display
            results.append(scored_session)

            # Print result
            flag = "🚨 FRAUD" if scored_session["fraud_flag"] else "✓  clean"
            print(
                f"\n[SCORED] {flag} | "
                f"Store: {scored_session['store_id']} | "
                f"Lane: {scored_session['lane_id']} | "
                f"Strategy: {scored_session['checkout_strategy']} | "
                f"Score: {scored_session['fraud_score']} | "
                f"Items: {scored_session['scanned_items']} scanned / "
                f"{scored_session['weighed_items']} weighed | "
                f"Voids: {scored_session['void_count']} | "
                f"Est. loss: ${scored_session['estimated_loss_usd']:.2f}"
            )

            if scored_session["fraud_flag"]:
                for signal in scored_session["signals_fired"]:
                    print(f"  ⚡ {signal['signal']}: {signal['detail']} (+{signal['score_add']} pts)")

            # Remove from store
            store.remove_session(lane_key)


# ─────────────────────────────────────────────────────────────────
#  MAIN — CONSUMER LOOP
# ─────────────────────────────────────────────────────────────────

def run_scorer():
    print("\n" + "─" * 60)
    print("  Walmart Shrink Pipeline — Session Scorer")
    print("─" * 60)
    print(f"  Kafka:           {KAFKA_BOOTSTRAP}")
    print(f"  Session gap:     {SESSION_GAP_SECS}s")
    print(f"  Fraud threshold: {FRAUD_THRESHOLD}")
    print("─" * 60 + "\n")

    # Init session store
    store   = SessionStore()
    results = []

    # Init Kafka producer for scored sessions output
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
    )

    # Init Kafka consumer for all 3 input topics
    consumer = KafkaConsumer(
        "pos.scan.events",
        "scale.weight.events",
        "pos.void.events",
        bootstrap_servers    = KAFKA_BOOTSTRAP,
        group_id             = "session-scorer",
        auto_offset_reset    = "latest",
        enable_auto_commit   = True,
        value_deserializer   = lambda m: json.loads(m.decode("utf-8")),
    )

    # Start background thread to close expired sessions
    closer_thread = threading.Thread(
        target=session_closer,
        args=(store, producer, results),
        daemon=True
    )
    closer_thread.start()

    print("[INFO] Listening for events. Sessions score after 90s inactivity.")
    print("[INFO] For faster testing, simulator runs at high fraud rate.\n")

    event_count = 0
    try:
        for message in consumer:
            topic = message.topic
            event = message.value

            # Build lane key for session grouping
            store_id = event.get("store_id", "UNKNOWN")
            lane_id  = event.get("lane_id", "UNKNOWN")
            lane_key = f"{store_id}:{lane_id}"

            # Add event to session store
            store.add_event(lane_key, topic, event)

            event_count += 1
            if event_count % 100 == 0:
                active = len(store.sessions)
                scored = len(results)
                print(f"[INFO] Events processed: {event_count} | Active sessions: {active} | Scored: {scored}")

    except KeyboardInterrupt:
        print(f"\n[INFO] Scorer stopped.")
        print(f"[INFO] Total events processed: {event_count}")
        print(f"[INFO] Total sessions scored:  {len(results)}")
        fraud_sessions = [r for r in results if r["fraud_flag"]]
        print(f"[INFO] Fraud sessions flagged:  {len(fraud_sessions)}")
    finally:
        consumer.close()
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_scorer()