"""
SOMSAT Dataset Generator
Generates realistic financial transaction fraud data for the SOMSAT-Kafka Hybrid Streaming Architecture.
Simulates normal transactions and various fraud typologies:
1. Impossible Travel (velocity jump in km)
2. Rapid High-Frequency Burst (bot / card stuffing)
3. Account Draining / Balance Anomalies
4. High-Risk Merchant Category & Low Device Trust Score
"""

import numpy as np
import pandas as pd
import random
from datetime import datetime, timedelta

def generate_somsat_dataset(n_samples=6000, fraud_ratio=0.045, random_state=42):
    np.random.seed(random_state)
    random.seed(random_state)

    categories = [
        "grocery", "restaurant", "electronics", "crypto_exchange",
        "luxury_retail", "travel_airline", "gaming_gambling", "utilities", "fashion", "atm_cash"
    ]
    high_risk_categories = ["crypto_exchange", "luxury_retail", "gaming_gambling", "electronics"]
    channels = ["UPI", "POS", "NET_BANKING", "ATM"]

    user_ids = [f"USR_{1000 + i}" for i in range(400)]
    records = []
    base_time = datetime(2026, 3, 1, 9, 0, 0)

    for i in range(n_samples):
        txn_id = f"TXN_SOM_{100000 + i}"
        user_id = random.choice(user_ids)
        timestamp = base_time + timedelta(seconds=i * random.randint(3, 12))
        is_fraud = 1 if np.random.rand() < fraud_ratio else 0

        if not is_fraud:
            # Legitimate transactions
            category = random.choices(categories, weights=[25, 20, 10, 2, 3, 5, 2, 18, 10, 5])[0]
            channel = random.choices(channels, weights=[45, 30, 15, 10])[0]
            amount = round(float(np.random.exponential(scale=1200) + 50), 2)
            amount = min(amount, 25000.0) # cap normal daily spends
            
            old_balance = round(float(np.random.uniform(amount * 1.5, amount * 10 + 5000)), 2)
            new_balance = round(max(0.0, old_balance - amount), 2)
            
            location_delta_km = round(float(np.random.exponential(scale=4.0)), 2) # usually close by
            device_trust_score = round(float(np.random.beta(a=9, b=1.5)), 3) # high trust: 0.7 - 0.99
            velocity_1h = int(np.random.choice([1, 2, 3, 4], p=[0.70, 0.20, 0.08, 0.02]))
        else:
            # Fraudulent transactions - multi-scenario anomalies
            fraud_type = random.choice(["impossible_travel", "account_drain", "burst_velocity", "untrusted_device"])
            
            if fraud_type == "impossible_travel":
                category = random.choice(high_risk_categories)
                channel = random.choice(["NET_BANKING", "UPI"])
                amount = round(float(np.random.uniform(15000, 120000)), 2)
                location_delta_km = round(float(np.random.uniform(450, 3200)), 2) # hundreds of km away!
                device_trust_score = round(float(np.random.uniform(0.1, 0.45)), 3)
                velocity_1h = random.randint(3, 7)
                old_balance = round(amount + np.random.uniform(1000, 30000), 2)
                new_balance = round(max(0.0, old_balance - amount), 2)

            elif fraud_type == "account_drain":
                category = random.choice(["crypto_exchange", "luxury_retail", "atm_cash"])
                channel = random.choice(["ATM", "NET_BANKING"])
                old_balance = round(float(np.random.uniform(50000, 300000)), 2)
                # Drains 90-99% of total balance
                amount = round(old_balance * np.random.uniform(0.90, 0.98), 2)
                new_balance = round(old_balance - amount, 2)
                location_delta_km = round(float(np.random.uniform(30, 400)), 2)
                device_trust_score = round(float(np.random.uniform(0.05, 0.35)), 3)
                velocity_1h = random.randint(2, 5)

            elif fraud_type == "burst_velocity":
                category = random.choice(["gaming_gambling", "crypto_exchange", "electronics"])
                channel = "UPI"
                amount = round(float(np.random.uniform(4000, 25000)), 2)
                location_delta_km = round(float(np.random.uniform(5, 80)), 2)
                device_trust_score = round(float(np.random.uniform(0.15, 0.50)), 3)
                velocity_1h = random.randint(9, 24) # Rapid burst of transactions!
                old_balance = round(float(np.random.uniform(30000, 100000)), 2)
                new_balance = round(max(0.0, old_balance - amount), 2)

            else: # untrusted_device
                category = random.choice(high_risk_categories)
                channel = random.choice(["NET_BANKING", "POS"])
                amount = round(float(np.random.uniform(20000, 95000)), 2)
                location_delta_km = round(float(np.random.uniform(100, 800)), 2)
                device_trust_score = round(float(np.random.uniform(0.01, 0.20)), 3) # emulator / rooted device
                velocity_1h = random.randint(4, 9)
                old_balance = round(amount + np.random.uniform(500, 15000), 2)
                new_balance = round(max(0.0, old_balance - amount), 2)

        records.append({
            "transaction_id": txn_id,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": user_id,
            "amount": amount,
            "old_balance": old_balance,
            "new_balance": new_balance,
            "merchant_category": category,
            "channel": channel,
            "location_delta_km": location_delta_km,
            "device_trust_score": device_trust_score,
            "velocity_1h": velocity_1h,
            "is_fraud": is_fraud
        })

    df = pd.DataFrame(records)
    return df

if __name__ == "__main__":
    df = generate_somsat_dataset()
    output_path = "/Users/ameyasurve/Desktop/moda_pjt/somsat-kafka/data/somsat.csv"
    df.to_csv(output_path, index=False)
    print(f"Generated SOMSAT dataset with {len(df)} transactions.")
    print(f"Fraud count: {df['is_fraud'].sum()} ({df['is_fraud'].mean()*100:.2f}%)")
    print(f"Saved to: {output_path}")
