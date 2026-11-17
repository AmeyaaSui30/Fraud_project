"""
Serving Layer: Real-Time Fraud Operations Dashboard
Framework: Streamlit + Plotly + SQLite Live Buffer
Architecture Role: Serving / Presentation Layer - Visualizes live Kafka streams, KPIs, and fraud alerts.
"""

import os
import time
import json
import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="SOMSAT | Financial Fraud Streaming Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "live_stream.db")
MODEL_META_PATH = os.path.join(os.path.dirname(__file__), "models", "model_metadata.json")

# Custom CSS for modern dark fintech styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&family=JetBrains+Mono:wght@400;600&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    .stMetric {
        background: #111827;
        border: 1px solid #1F2937;
        padding: 16px 20px;
        border-radius: 12px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 9999px;
        font-size: 0.82rem;
        font-weight: 600;
        letter-spacing: 0.03em;
    }
    .badge-online { background-color: rgba(16, 185, 129, 0.2); color: #10B981; border: 1px solid #10B981; }
    .badge-fraud { background-color: rgba(239, 68, 68, 0.2); color: #EF4444; border: 1px solid #EF4444; }
    .badge-norm { background-color: rgba(59, 130, 246, 0.2); color: #3B82F6; border: 1px solid #3B82F6; }

    .custom-card {
        background-color: #111827;
        border: 1px solid #1F2937;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 20px;
    }
    
    .metric-title {
        font-size: 0.85rem;
        color: #9CA3AF;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 700;
        color: #F9FAFB;
    }
    .metric-sub {
        font-size: 0.78rem;
        color: #6B7280;
        margin-top: 4px;
    }
</style>
""", unsafe_allow_html=True)

def get_db_data(limit=500):
    """Fetches recent transactions from SQLite live buffer."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(DB_PATH, timeout=3.0)
        query = f"SELECT * FROM transactions ORDER BY id DESC LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

def get_model_metadata():
    if os.path.exists(MODEL_META_PATH):
        try:
            with open(MODEL_META_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def inject_kafka_transaction(record: dict):
    """Directly pushes a simulation transaction to Kafka topic."""
    from kafka import KafkaProducer
    try:
        producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        record["event_timestamp"] = time.time()
        record["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        producer.send("somsat-transactions", value=record)
        producer.flush()
        producer.close()
        return True, "Transaction published to Kafka topic 'somsat-transactions'!"
    except Exception as e:
        return False, f"Failed to connect to Kafka: {e}"

# Sidebar Controls
st.sidebar.title("🛡️ SOMSAT Controls")
st.sidebar.markdown("**Hybrid Streaming Architecture**")
st.sidebar.markdown("""
- **Broker**: `localhost:9092`
- **Topic**: `somsat-transactions`
- **Model**: `Isolation Forest`
""")

auto_refresh = st.sidebar.toggle("⚡ Live Auto-Refresh", value=True)
refresh_rate = st.sidebar.slider("Refresh Interval (seconds)", min_value=1, max_value=5, value=2)

st.sidebar.markdown("---")
st.sidebar.subheader("🧪 Live Fraud Injection Tool")
st.sidebar.caption("Simulate real-time attacks to verify low-latency detection.")

with st.sidebar.form("simulator_form"):
    sim_user = st.text_input("User ID", value="USR_9999")
    sim_amount = st.number_input("Amount (₹)", min_value=10.0, max_value=500000.0, value=95000.0, step=500.0)
    sim_old_balance = st.number_input("Old Balance (₹)", min_value=0.0, max_value=1000000.0, value=100000.0, step=1000.0)
    sim_new_balance = st.number_input("New Balance (₹)", min_value=0.0, max_value=1000000.0, value=5000.0, step=1000.0)
    sim_category = st.selectbox("Merchant Category", [
        "crypto_exchange", "luxury_retail", "electronics", "gaming_gambling", 
        "grocery", "restaurant", "travel_airline", "atm_cash"
    ])
    sim_channel = st.selectbox("Channel", ["UPI", "NET_BANKING", "POS", "ATM"])
    sim_location_km = st.slider("Location Delta (km)", min_value=0, max_value=3000, value=1250)
    sim_device_trust = st.slider("Device Trust Score", min_value=0.0, max_value=1.0, value=0.08, step=0.01)
    sim_velocity = st.slider("1-Hour Velocity", min_value=1, max_value=25, value=12)
    
    submitted = st.form_submit_button("🚀 Inject Transaction to Kafka")
    if submitted:
        test_payload = {
            "transaction_id": f"TXN_MANUAL_{int(time.time())}",
            "user_id": sim_user,
            "amount": sim_amount,
            "old_balance": sim_old_balance,
            "new_balance": sim_new_balance,
            "merchant_category": sim_category,
            "channel": sim_channel,
            "location_delta_km": sim_location_km,
            "device_trust_score": sim_device_trust,
            "velocity_1h": sim_velocity,
            "is_fraud": 1 if (sim_amount > 40000 or sim_location_km > 300 or sim_device_trust < 0.3) else 0
        }
        ok, msg = inject_kafka_transaction(test_payload)
        if ok:
            st.sidebar.success(msg)
        else:
            st.sidebar.error(msg)

st.sidebar.markdown("---")
st.sidebar.markdown(
    "[🌐 Open Kafka Web UI](http://localhost:8080)  \n"
    "*Cluster inspection, topic lags & partition offsets*"
)

# Header Section
col_title, col_status = st.columns([3, 1])
with col_title:
    st.title("Low-Latency Financial Fraud Detection")
    st.caption("SOMSAT Dataset • Hybrid Streaming Architecture (Speed Layer: Kafka + Batch Layer: Isolation Forest)")

with col_status:
    st.markdown("""
        <div style="text-align: right; padding-top: 15px;">
            <span class="status-badge badge-online">● KAFKA BROKER: ACTIVE</span>
        </div>
    """, unsafe_allow_html=True)

# Fetch current stream data
df_data = get_db_data(limit=600)
meta = get_model_metadata()

if df_data.empty:
    st.warning("⚠️ Waiting for stream data... Start `speed_consumer.py` and `producer.py` in your terminal to view real-time metrics!")
    st.info("Quick Start:\n1. `python producer.py`\n2. `python speed_consumer.py`")
    if auto_refresh:
        time.sleep(refresh_rate)
        st.rerun()
    st.stop()

# Key Performance Indicators
total_txns = len(df_data)
frauds_flagged = int(df_data["is_fraud_predicted"].sum())
fraud_rate = (frauds_flagged / max(1, total_txns)) * 100.0
avg_latency = float(df_data["latency_ms"].mean())
p99_latency = float(np.percentile(df_data["latency_ms"], 99)) if total_txns > 5 else avg_latency

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
with kpi1:
    st.markdown(f"""
        <div class="custom-card">
            <div class="metric-title">Total Ingested</div>
            <div class="metric-value">{total_txns:,}</div>
            <div class="metric-sub">Streaming events received</div>
        </div>
    """, unsafe_allow_html=True)

with kpi2:
    st.markdown(f"""
        <div class="custom-card">
            <div class="metric-title">Frauds Intercepted</div>
            <div class="metric-value" style="color: #EF4444;">{frauds_flagged}</div>
            <div class="metric-sub">Flagged in real-time</div>
        </div>
    """, unsafe_allow_html=True)

with kpi3:
    st.markdown(f"""
        <div class="custom-card">
            <div class="metric-title">Fraud Incidence</div>
            <div class="metric-value" style="color: #F59E0B;">{fraud_rate:.2f}%</div>
            <div class="metric-sub">Current stream proportion</div>
        </div>
    """, unsafe_allow_html=True)

with kpi4:
    st.markdown(f"""
        <div class="custom-card">
            <div class="metric-title">Avg Latency</div>
            <div class="metric-value" style="color: #10B981;">{avg_latency:.2f} ms</div>
            <div class="metric-sub">Sub-millisecond inference</div>
        </div>
    """, unsafe_allow_html=True)

with kpi5:
    st.markdown(f"""
        <div class="custom-card">
            <div class="metric-title">P99 Tail Latency</div>
            <div class="metric-value" style="color: #6366F1;">{p99_latency:.2f} ms</div>
            <div class="metric-sub">99th percentile SLA</div>
        </div>
    """, unsafe_allow_html=True)

# Visualizations Row
chart_col1, chart_col2 = st.columns([1, 1])

with chart_col1:
    st.markdown("##### ⏱️ Real-Time Latency Timeline (ms)")
    timeline_df = df_data.head(50).sort_values("id")
    fig_lat = go.Figure()
    fig_lat.add_trace(go.Scatter(
        x=list(range(len(timeline_df))),
        y=timeline_df["latency_ms"],
        mode="lines+markers",
        name="Latency (ms)",
        line=dict(color="#10B981", width=2),
        marker=dict(size=4)
    ))
    fig_lat.add_hline(
        y=10.0, line_dash="dash", line_color="#EF4444",
        annotation_text="10ms SLA Upper Bound", annotation_position="top left"
    )
    fig_lat.update_layout(
        height=280,
        margin=dict(l=20, r=20, t=20, b=20),
        plot_bgcolor="#111827",
        paper_bgcolor="#111827",
        font=dict(color="#9CA3AF"),
        xaxis=dict(showgrid=False, title="Recent Events"),
        yaxis=dict(gridcolor="#1F2937", title="Latency (ms)")
    )
    st.plotly_chart(fig_lat, use_container_width=True)

with chart_col2:
    st.markdown("##### 🚨 Fraud Distribution by Merchant Category")
    fraud_only = df_data[df_data["is_fraud_predicted"] == 1]
    if not fraud_only.empty:
        cat_counts = fraud_only["merchant_category"].value_counts().reset_index()
        cat_counts.columns = ["Category", "Count"]
        fig_cat = px.pie(
            cat_counts,
            values="Count",
            names="Category",
            hole=0.45,
            color_discrete_sequence=px.colors.sequential.Sunsetdark
        )
        fig_cat.update_layout(
            height=280,
            margin=dict(l=10, r=10, t=10, b=10),
            plot_bgcolor="#111827",
            paper_bgcolor="#111827",
            font=dict(color="#9CA3AF"),
            showlegend=True
        )
        st.plotly_chart(fig_cat, use_container_width=True)
    else:
        st.info("No fraudulent transactions detected in the current buffer yet.")

# Anomaly Scatter Plot & Architecture Info Row
col_scatter, col_arch = st.columns([1.2, 0.8])

with col_scatter:
    st.markdown("##### 🔍 Multi-Dimensional Anomaly Cluster (Amount vs Risk Score)")
    plot_df = df_data.head(150).copy()
    plot_df["Status"] = plot_df["is_fraud_predicted"].map({1: "Fraud Alert", 0: "Legitimate"})
    fig_scatter = px.scatter(
        plot_df,
        x="amount",
        y="anomaly_score",
        color="Status",
        color_discrete_map={"Fraud Alert": "#EF4444", "Legitimate": "#3B82F6"},
        hover_data=["transaction_id", "user_id", "merchant_category", "fraud_reason"],
        labels={"amount": "Amount (₹)", "anomaly_score": "Risk Anomaly Score"}
    )
    fig_scatter.update_layout(
        height=280,
        margin=dict(l=20, r=20, t=20, b=20),
        plot_bgcolor="#111827",
        paper_bgcolor="#111827",
        font=dict(color="#9CA3AF"),
        xaxis=dict(gridcolor="#1F2937"),
        yaxis=dict(gridcolor="#1F2937")
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_arch:
    st.markdown("##### 🏛️ Hybrid Streaming Architecture")
    st.markdown("""
    ```
      +-------------------------------------------+
      |  SOMSAT Historical Data (data/somsat.csv) |
      +---------------------+---------------------+
                            |
                            v [Batch Layer]
             +------------------------------+
             | Isolation Forest (model.pkl) |
             +--------------+---------------+
                            |
                            v [Speed Layer Inference]
    [Producer] ---> [Kafka Topic] ---> [Speed Consumer]
    (Replay Stream)   (9092)          (< 5ms Latency)
                                            |
                                            v
                                  [Alerts + Live UI]
    ```
    """)
    if meta:
        st.caption(f"Batch ROC-AUC: **{meta['metrics']['roc_auc']}** | F1-Score: **{meta['metrics']['f1_score']}**")

# Live Transactions Table
st.markdown("##### 📋 Live Processed Transactions Feed")
display_df = df_data.head(20)[[
    "transaction_id", "user_id", "amount", "merchant_category", 
    "channel", "device_trust_score", "anomaly_score", "latency_ms", 
    "is_fraud_predicted", "fraud_reason"
]].copy()

display_df.rename(columns={
    "transaction_id": "Txn ID",
    "user_id": "User",
    "amount": "Amount (₹)",
    "merchant_category": "Category",
    "channel": "Channel",
    "device_trust_score": "Trust",
    "anomaly_score": "Risk Score",
    "latency_ms": "Latency (ms)",
    "is_fraud_predicted": "Verdict",
    "fraud_reason": "Anomaly Explanations"
}, inplace=True)

display_df["Verdict"] = display_df["Verdict"].map({1: "🚨 FRAUD", 0: "✅ NORMAL"})
st.dataframe(display_df, use_container_width=True, height=350)

# Auto-refresh loop
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
