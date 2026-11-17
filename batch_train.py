"""
Batch Layer: Model Training for SOMSAT Financial Fraud Detection
Framework: Scikit-Learn (Isolation Forest & Preprocessing Pipeline)
Architecture Role: Batch Layer (Historical Analytics & Anomaly Pattern Extraction)
"""

import os
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, roc_auc_score, precision_recall_fscore_support

MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "somsat.csv")
MODEL_FILE = os.path.join(MODEL_DIR, "model.pkl")
METADATA_FILE = os.path.join(MODEL_DIR, "model_metadata.json")

HIGH_RISK_CATEGORIES = {"crypto_exchange", "luxury_retail", "gaming_gambling", "electronics"}

def extract_features(df: pd.DataFrame):
    """
    Feature engineering pipeline for SOMSAT financial transactions.
    Creates domain-specific financial fraud signals.
    """
    df_feat = pd.DataFrame()
    
    df_feat["amount"] = df["amount"].astype(float)
    df_feat["old_balance"] = df["old_balance"].astype(float)
    df_feat["new_balance"] = df["new_balance"].astype(float)
    
    # Feature 1: Balance Drain Ratio: proportion of balance depleted
    balance_diff = (df["old_balance"] - df["new_balance"]).clip(lower=0)
    df_feat["balance_drain_ratio"] = balance_diff / (df["old_balance"] + 1.0)
    
    # Feature 2: High velocity indicator (burst transactions in last 1 hour)
    df_feat["velocity_1h"] = df["velocity_1h"].astype(float)
    
    # Feature 3: Geolocation jump (impossible travel distance)
    df_feat["location_delta_km"] = df["location_delta_km"].astype(float)
    
    # Feature 4: Device Trust Score (inverted to untrusted score for anomaly alignment)
    df_feat["device_trust_score"] = df["device_trust_score"].astype(float)
    df_feat["device_untrust_score"] = 1.0 - df_feat["device_trust_score"]
    
    # Feature 5: Category Risk Flag (binary)
    df_feat["is_high_risk_cat"] = df["merchant_category"].apply(
        lambda c: 1.0 if str(c) in HIGH_RISK_CATEGORIES else 0.0
    )
    
    # Feature 6: Amount to Velocity Interaction
    df_feat["amount_velocity_interaction"] = df_feat["amount"] * (df_feat["velocity_1h"] + 1.0)

    # Feature 7: Digital Channel Flag
    df_feat["is_digital_channel"] = df["channel"].apply(
        lambda ch: 1.0 if ch in ["UPI", "NET_BANKING"] else 0.0
    )
    
    feature_columns = list(df_feat.columns)
    return df_feat, feature_columns

def train_batch_model():
    print("=" * 60)
    print("SOMSAT KAFKA HYBRID STREAMING - BATCH LAYER TRAINING")
    print("=" * 60)

    os.makedirs(MODEL_DIR, exist_ok=True)
    
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Dataset not found at {DATA_PATH}. Run generate_dataset.py first!")

    print(f"Loading historical dataset: {DATA_PATH}")
    df = pd.read_csv(DATA_PATH)
    print(f"Dataset loaded: {len(df)} transactions.")
    fraud_rate = df["is_fraud"].mean() * 100
    print(f"Historical Fraud Distribution: {df['is_fraud'].sum()} frauds ({fraud_rate:.2f}%)")

    # Feature Engineering
    X, feature_columns = extract_features(df)
    y_true = df["is_fraud"].values

    print(f"\nEngineered {len(feature_columns)} features:")
    for col in feature_columns:
        print(f"  - {col}")

    # Standard Scaler
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train Isolation Forest (Anomaly Detection Model)
    # Contamination set according to historical expected fraud rate
    contamination_rate = max(0.02, min(0.10, df["is_fraud"].mean()))
    print(f"\nTraining Isolation Forest (Contamination={contamination_rate:.4f}, n_estimators=150)...")
    
    iso_forest = IsolationForest(
        n_estimators=150,
        max_samples="auto",
        contamination=contamination_rate,
        random_state=42,
        n_jobs=-1
    )
    iso_forest.fit(X_scaled)

    # Evaluate Model
    # IsolationForest returns -1 for outliers/frauds and 1 for inliers/normal
    raw_preds = iso_forest.predict(X_scaled)
    y_pred = np.where(raw_preds == -1, 1, 0)
    
    # Anomaly scores: lower decision_function value = more anomalous
    scores = iso_forest.decision_function(X_scaled)
    # Normalized anomaly score from 0.0 (safest) to 1.0 (highest fraud risk)
    anomaly_scores = -scores
    normalized_risk = (anomaly_scores - anomaly_scores.min()) / (anomaly_scores.max() - anomaly_scores.min() + 1e-9)

    precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="binary")
    roc_auc = roc_auc_score(y_true, normalized_risk)

    print("\nBatch Model Performance Evaluation:")
    print("-" * 45)
    print(f"ROC-AUC Score : {roc_auc:.4f}")
    print(f"Precision     : {precision:.4f}")
    print(f"Recall        : {recall:.4f}")
    print(f"F1-Score      : {f1:.4f}")
    print("-" * 45)
    print("\nClassification Report:")
    print(classification_report(y_true, y_pred, target_names=["Legitimate (0)", "Fraud (1)"]))

    # Save Pipeline Bundle
    pipeline_artifact = {
        "model": iso_forest,
        "scaler": scaler,
        "feature_columns": feature_columns,
        "contamination": contamination_rate,
        "score_min": float(anomaly_scores.min()),
        "score_max": float(anomaly_scores.max())
    }
    
    joblib.dump(pipeline_artifact, MODEL_FILE)
    print(f"Saved model pipeline to: {MODEL_FILE}")

    # Save Metadata JSON
    metadata = {
        "model_type": "Isolation Forest (Unsupervised Anomaly Detector)",
        "features": feature_columns,
        "metrics": {
            "roc_auc": round(float(roc_auc), 4),
            "precision": round(float(precision), 4),
            "recall": round(float(recall), 4),
            "f1_score": round(float(f1), 4),
            "total_samples": int(len(df)),
            "fraud_ratio_pct": round(float(fraud_rate), 2)
        },
        "score_scaling": {
            "score_min": float(anomaly_scores.min()),
            "score_max": float(anomaly_scores.max())
        }
    }
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to: {METADATA_FILE}")
    print("=" * 60)
    print("Batch Layer Training Completed Successfully!")
    print("=" * 60)

if __name__ == "__main__":
    train_batch_model()
