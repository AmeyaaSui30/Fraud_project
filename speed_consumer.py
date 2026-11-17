"""
Speed Layer: Real-Time Fraud Detection Consumer & Low-Latency Benchmark
Topic: somsat-transactions
Alert Topic: somsat-fraud-alerts
Framework: Apache Kafka + Scikit-Learn Isolation Forest + SQLite Live Store
Architecture Role: Speed Layer - Real-time stream processing, microsecond inference, latency telemetry.
"""

import os
import sys
import time
import json
import sqlite3
import argparse
import joblib
import numpy as np
import pandas as pd
from datetime import datetime
from collections import deque
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
INPUT_TOPIC = "somsat-transactions"
ALERT_TOPIC = "somsat-fraud-alerts"
MODEL_PATH = os.path.join(os.path.dirname(__file__), "models", "model.pkl")
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "live_stream.db")

HIGH_RISK_CATEGORIES = {"crypto_exchange", "luxury_retail", "gaming_gambling", "electronics"}

def init_live_db():
    """Initializes high-concurrency SQLite store for Streamlit dashboard access."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    cursor = conn.cursor()
    
    # Enable WAL mode for zero-lock concurrent reads by Streamlit dashboard
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA synchronous=NORMAL;")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT,
            timestamp TEXT,
            user_id TEXT,
            amount REAL,
            old_balance REAL,
            new_balance REAL,
            merchant_category TEXT,
            channel TEXT,
            location_delta_km REAL,
            device_trust_score REAL,
            velocity_1h INTEGER,
            is_fraud_ground_truth INTEGER,
            is_fraud_predicted INTEGER,
            anomaly_score REAL,
            latency_ms REAL,
            fraud_reason TEXT,
            processed_at TEXT
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_txn_id ON transactions(transaction_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fraud ON transactions(is_fraud_predicted);")
    conn.commit()
    conn.close()

def save_transaction_to_db(record_data):
    """Inserts processed transaction and latency telemetry into SQLite."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=5.0)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO transactions (
                transaction_id, timestamp, user_id, amount, old_balance, new_balance,
                merchant_category, channel, location_delta_km, device_trust_score,
                velocity_1h, is_fraud_ground_truth, is_fraud_predicted, anomaly_score,
                latency_ms, fraud_reason, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, record_data)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Warning: Failed to persist to DB: {e}")

def determine_fraud_reasons(row: dict) -> str:
    """Explains why a transaction is flagged as suspicious."""
    reasons = []
    amt = float(row.get("amount", 0))
    old_b = float(row.get("old_balance", 0))
    new_b = float(row.get("new_balance", 0))
    loc_km = float(row.get("location_delta_km", 0))
    trust = float(row.get("device_trust_score", 1.0))
    vel = int(row.get("velocity_1h", 1))
    cat = str(row.get("merchant_category", ""))

    if loc_km > 350:
        reasons.append(f"Impossible Travel ({loc_km:.0f} km)")
    if vel >= 6:
        reasons.append(f"High Velocity Burst ({vel} txns/hr)")
    if trust < 0.35:
        reasons.append(f"Untrusted Device ({trust:.2f})")
    if old_b > 0 and ((old_b - new_b) / old_b) > 0.85 and amt > 10000:
        reasons.append("Account Drain Spike")
    if cat in HIGH_RISK_CATEGORIES and amt > 25000:
        reasons.append(f"High-Risk Merchant ({cat})")
    
    if not reasons:
        reasons.append("Multi-Variate Statistical Anomaly")
    return " | ".join(reasons)

def load_pipeline():
    """Loads batch trained Isolation Forest model bundle."""
    if not os.path.exists(MODEL_PATH):
        print(f"Error: Model file {MODEL_PATH} not found. Run 'batch_train.py' first!")
        sys.exit(1)
    
    print(f"Loading trained Batch Layer Model from {MODEL_PATH}...")
    bundle = joblib.load(MODEL_PATH)
    print("Model loaded successfully!")
    return bundle

def process_features(record: dict, feature_columns: list, scaler):
    """Transforms streaming record dictionary into normalized feature vector."""
    amt = float(record["amount"])
    old_b = float(record["old_balance"])
    new_b = float(record["new_balance"])
    vel = float(record["velocity_1h"])
    loc_km = float(record["location_delta_km"])
    trust = float(record["device_trust_score"])
    cat = str(record["merchant_category"])
    ch = str(record["channel"])

    drain_ratio = max(0.0, (old_b - new_b)) / (old_b + 1.0)
    untrust = 1.0 - trust
    is_high_risk = 1.0 if cat in HIGH_RISK_CATEGORIES else 0.0
    amt_vel_int = amt * (vel + 1.0)
    is_digital = 1.0 if ch in ["UPI", "NET_BANKING"] else 0.0

    feat_dict = {
        "amount": amt,
        "old_balance": old_b,
        "new_balance": new_b,
        "balance_drain_ratio": drain_ratio,
        "velocity_1h": vel,
        "location_delta_km": loc_km,
        "device_trust_score": trust,
        "device_untrust_score": untrust,
        "is_high_risk_cat": is_high_risk,
        "amount_velocity_interaction": amt_vel_int,
        "is_digital_channel": is_digital
    }

    # Order correctly according to training features with proper column names
    df_row = pd.DataFrame([{col: feat_dict[col] for col in feature_columns}], columns=feature_columns)
    scaled_vector = scaler.transform(df_row)
    return scaled_vector, df_row

def main():
    parser = argparse.ArgumentParser(description="SOMSAT Speed Layer Consumer")
    parser.add_argument("--broker", type=str, default=KAFKA_BROKER, help="Kafka broker address")
    args = parser.parse_args()

    init_live_db()
    bundle = load_pipeline()
    model = bundle["model"]
    scaler = bundle["scaler"]
    feature_columns = bundle["feature_columns"]
    score_min = bundle.get("score_min", -0.5)
    score_max = bundle.get("score_max", 0.5)

    print(f"Connecting Consumer to Kafka Broker '{args.broker}'...")
    consumer = None
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=[args.broker],
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="somsat-speed-detector-group",
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            print("Kafka Consumer connected!")
            break
        except NoBrokersAvailable:
            print(f"[{attempt}/10] Waiting for Kafka Broker... retry in 3s")
            time.sleep(3)

    if not consumer:
        print("Fatal: Could not connect to Kafka Consumer.")
        sys.exit(1)

    # Secondary Producer for Publishing Fraud Alerts
    alert_producer = None
    try:
        alert_producer = KafkaProducer(
            bootstrap_servers=[args.broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print(f"Alert Producer initialized for topic '{ALERT_TOPIC}'.")
    except Exception as e:
        print(f"Notice: Running without alert producer: {e}")

    print("=" * 70)
    print("⚡ SPEED LAYER: REAL-TIME LOW-LATENCY DETECTION CONSUMER ACTIVE")
    print(f"Subscribed Topic   : {INPUT_TOPIC}")
    print(f"Alert Topic        : {ALERT_TOPIC}")
    print(f"Serving Model      : Isolation Forest")
    print("Awaiting transactions...")
    print("=" * 70)

    latency_window = deque(maxlen=200)
    total_processed = 0
    fraud_detected = 0

    try:
        for message in consumer:
            t_start_process = time.time()
            record = message.value
            
            # 1. Compute End-to-End Latency
            event_ts = record.get("event_timestamp", t_start_process)
            latency_ms = (time.time() - event_ts) * 1000.0
            latency_ms = max(0.01, latency_ms) # prevent non-positive edge cases
            latency_window.append(latency_ms)

            # 2. Extract Features & Predict
            X_feat, _ = process_features(record, feature_columns, scaler)
            
            # IsolationForest prediction (-1 = fraud, 1 = normal)
            raw_pred = model.predict(X_feat)[0]
            is_fraud_pred = 1 if raw_pred == -1 else 0
            
            # Anomaly Score Normalization
            raw_score = model.decision_function(X_feat)[0]
            anomaly_score = -raw_score
            normalized_risk = float(np.clip(
                (anomaly_score - score_min) / (score_max - score_min + 1e-9), 0.0, 1.0
            ))

            total_processed += 1
            reason = ""

            if is_fraud_pred == 1:
                fraud_detected += 1
                reason = determine_fraud_reasons(record)
                
                # Publish to Kafka Alert Topic
                if alert_producer:
                    alert_payload = {
                        "alert_id": f"ALT_{total_processed}",
                        "transaction_id": record["transaction_id"],
                        "user_id": record["user_id"],
                        "amount": record["amount"],
                        "merchant_category": record["merchant_category"],
                        "risk_score": round(normalized_risk, 3),
                        "reasons": reason,
                        "latency_ms": round(latency_ms, 2),
                        "timestamp": datetime.now().isoformat()
                    }
                    alert_producer.send(ALERT_TOPIC, value=alert_payload)

            # 3. Persist to Live DB for Dashboard
            db_row = (
                record.get("transaction_id", f"TXN_{total_processed}"),
                record.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                record.get("user_id", "USR_UNKNOWN"),
                float(record.get("amount", 0)),
                float(record.get("old_balance", 0)),
                float(record.get("new_balance", 0)),
                str(record.get("merchant_category", "unknown")),
                str(record.get("channel", "unknown")),
                float(record.get("location_delta_km", 0)),
                float(record.get("device_trust_score", 1.0)),
                int(record.get("velocity_1h", 1)),
                int(record.get("is_fraud", 0)),
                int(is_fraud_pred),
                round(float(normalized_risk), 4),
                round(float(latency_ms), 3),
                reason,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            save_transaction_to_db(db_row)

            # 4. Live Terminal Output & Telemetry
            avg_lat = np.mean(latency_window) if latency_window else 0.0
            p99_lat = np.percentile(latency_window, 99) if len(latency_window) > 10 else latency_ms

            if is_fraud_pred == 1:
                print(
                    f"\033[91m🚨 [FRAUD DETECTED]\033[0m "
                    f"Txn: {record.get('transaction_id')} | "
                    f"User: {record.get('user_id')} | "
                    f"₹{float(record.get('amount', 0)):,.2f} | "
                    f"Risk: {normalized_risk:.2f} | "
                    f"Latency: \033[93m{latency_ms:.2f} ms\033[0m | "
                    f"Reason: {reason}"
                )
            else:
                if total_processed % 5 == 0 or total_processed < 20:
                    print(
                        f"\033[92m✅ [NORMAL]\033[0m         "
                        f"Txn: {record.get('transaction_id')} | "
                        f"User: {record.get('user_id')} | "
                        f"₹{float(record.get('amount', 0)):,.2f} | "
                        f"Risk: {normalized_risk:.2f} | "
                        f"Latency: {latency_ms:.2f} ms | "
                        f"Avg/P99: {avg_lat:.2f}/{p99_lat:.2f} ms"
                    )

    except KeyboardInterrupt:
        print("\n\nConsumer stopped by user (Ctrl+C).")
    finally:
        consumer.close()
        if alert_producer:
            alert_producer.flush()
            alert_producer.close()
        print("=" * 70)
        print("SPEED LAYER SHUTDOWN SUMMARY:")
        print(f"Total Transactions Processed : {total_processed}")
        print(f"Total Frauds Intercepted     : {fraud_detected} ({(fraud_detected/max(1, total_processed))*100:.2f}%)")
        if latency_window:
            print(f"Average Latency              : {np.mean(latency_window):.2f} ms")
            print(f"P95 Latency                  : {np.percentile(latency_window, 95):.2f} ms")
            print(f"P99 Latency                  : {np.percentile(latency_window, 99):.2f} ms")
        print("=" * 70)

if __name__ == "__main__":
    main()
