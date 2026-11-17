"""
Speed Layer: Transaction Stream Producer
Topic: somsat-transactions
Framework: Apache Kafka + Python (kafka-python-ng)
Architecture Role: Speed Layer - Ingests and replays financial transactions with high-precision timestamps.
"""

import os
import sys
import time
import json
import random
import argparse
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "somsat-transactions"
DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "somsat.csv")

def create_kafka_producer(broker_address=KAFKA_BROKER, retries=10, delay_sec=3):
    """Initializes Kafka Producer with exponential retry for container startup."""
    print(f"Connecting Kafka Producer to broker at '{broker_address}'...")
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker_address],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=1
            )
            print(f"Kafka Producer successfully connected on attempt {attempt}!")
            return producer
        except NoBrokersAvailable:
            print(f"[{attempt}/{retries}] Kafka broker not ready yet. Retrying in {delay_sec}s...")
            time.sleep(delay_sec)
    
    print("Could not connect to Kafka Broker. Ensure Docker container is running (`docker compose up -d`).")
    sys.exit(1)

def stream_transactions(producer, df: pd.DataFrame, delay: float = 0.15, continuous: bool = True, max_count: int = None):
    """
    Streams transactions from SOMSAT dataset into the Kafka topic.
    Attaches real-time event timestamp for low-latency benchmark calculation.
    """
    total_records = len(df)
    sent_count = 0
    cycle = 0

    print("=" * 65)
    print("🚀 SOMSAT TRANSACTION STREAM PRODUCER STARTED")
    print(f"Target Kafka Topic : {TOPIC_NAME}")
    print(f"Streaming Delay    : {delay}s per transaction (~{int(1/delay) if delay > 0 else 'MAX'} txns/sec)")
    print(f"Loop Mode          : {'Infinite Continuous Stream' if continuous else 'Single Dataset Pass'}")
    print("Press Ctrl+C to terminate.")
    print("=" * 65)

    try:
        while True:
            cycle += 1
            shuffled_df = df.sample(frac=1.0).reset_index(drop=True)
            
            for _, row in shuffled_df.iterrows():
                # Format payload
                record = row.to_dict()
                
                # In continuous cycles, generate unique dynamic transaction IDs and fresh timestamps
                if cycle > 1:
                    record["transaction_id"] = f"TXN_LIVE_{random.randint(200000, 999999)}"
                    record["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Attach high-resolution ingestion timestamp to calculate end-to-end latency
                record["event_timestamp"] = time.time()
                
                # Publish to Kafka
                producer.send(TOPIC_NAME, value=record)
                sent_count += 1

                # Visual console status
                status_icon = "⚠️ [FRAUD]" if record.get("is_fraud") == 1 else "✅ [NORM ]"
                print(
                    f"{status_icon} #{sent_count:05d} | "
                    f"ID: {record['transaction_id']} | "
                    f"User: {record['user_id']} | "
                    f"Amt: ₹{float(record['amount']):>9.2f} | "
                    f"Cat: {record['merchant_category']:<15} | "
                    f"Chan: {record['channel']:<11}"
                )

                if delay > 0:
                    time.sleep(delay)

                if max_count and sent_count >= max_count:
                    print(f"\nReached max_count limit of {max_count} transactions.")
                    return

            if not continuous:
                break

    except KeyboardInterrupt:
        print("\n\nProducer stopped by user (Ctrl+C). Flushing messages...")
    finally:
        producer.flush()
        producer.close()
        print(f"Producer closed. Total transactions sent to Kafka: {sent_count}")

def main():
    parser = argparse.ArgumentParser(description="SOMSAT Kafka Live Transaction Stream Producer")
    parser.add_argument("--broker", type=str, default=KAFKA_BROKER, help="Kafka broker host:port")
    parser.add_argument("--delay", type=float, default=0.15, help="Delay between transactions in seconds (default: 0.15s)")
    parser.add_argument("--once", action="store_true", help="Stream dataset once without continuous loop")
    parser.add_argument("--count", type=int, default=None, help="Stop after sending N transactions")
    args = parser.parse_args()

    if not os.path.exists(DATA_PATH):
        print(f"Error: Dataset {DATA_PATH} not found. Run batch_train.py or generate_dataset.py first.")
        sys.exit(1)

    df = pd.read_csv(DATA_PATH)
    producer = create_kafka_producer(broker_address=args.broker)
    stream_transactions(
        producer=producer,
        df=df,
        delay=args.delay,
        continuous=not args.once,
        max_count=args.count
    )

if __name__ == "__main__":
    main()
