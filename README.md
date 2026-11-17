# Low-Latency Financial Fraud Detection via Hybrid Streaming Architecture (SOMSAT-Kafka)

---
## 📖 1. Project Overview & Architecture Explanation (Project Samjho)

### 📌 Problem Statement:
Banks aur financial institutions (UPI, Credit Cards, Net Banking) me daily millions of transactions hote hain. Traditional batch processing systems (jaise nightly batch jobs) fraud ko ghanto baad detect karte hain, tab tak paise transfer ho chuke hote hain.
Hame chahiye **Sub-Millisecond (< 5-10ms) Low-Latency Fraud Detection System** jo transaction complete hone se pehle ya real-time stream me hi anomaly flag kar sake.

### 🏛️ Hybrid Streaming Architecture (Lambda / Hybrid Architecture):
Hamara system 3 layers me structured hai:

1. **Batch Layer (`batch_train.py`):**
   - Historical financial dataset (`data/somsat.csv`) ko analyze karta hai.
   - Deep feature engineering (velocity, impossible travel, balance drain ratio, device trust score) perform karta hai.
   - **Isolation Forest** model train karke normal vs anomalous spending patterns seekhta hai.
   - Output: `models/model.pkl` (production-ready inference pipeline) generate karta hai with **ROC-AUC > 0.99**.

2. **Speed Layer (`producer.py` + Kafka + `speed_consumer.py`):**
   - **Apache Kafka (Docker Broker: port 9092)**: High-throughput, distributed streaming backbone.
   - **Producer (`producer.py`)**: Real-world transactions replay karta hai with microsecond event timestamps.
   - **Speed Consumer (`speed_consumer.py`)**: Real-time Kafka topic `somsat-transactions` se messages read karta hai, trained model se **< 5ms inference** nikalta hai, end-to-end latency calculate karta hai, aur high-risk alerts ko `somsat-fraud-alerts` topic me push karta hai.

3. **Serving & Presentation Layer (`dashboard.py` + Kafka UI):**
   - **Streamlit Dashboard (port 8501)**: Live monitoring UI with interactive KPIs, latency graphs, fraud distribution, aur **Live Fraud Injection Tool**.
   - **Kafka UI (port 8080)**: Web-based console to monitor Kafka clusters, topics, consumer lags, aur partition offsets.

```
                                  HYBRID ARCHITECTURE FLOW
                                  ========================

   [ Historical Data ]
    data/somsat.csv
          |
          v
   +---------------+
   |  BATCH LAYER  |  ---> Trains Unsupervised Isolation Forest Pipeline
   | batch_train.py|  ---> Generates models/model.pkl & metadata
   +---------------+
          |
          | (Pre-trained weights)
          v
   +-------------------------------------------------------------------------+
   |                              SPEED LAYER                                |
   |                                                                         |
   |   [ Producer ]  --replays txns-->  [ Apache Kafka ]                     |
   |   producer.py                      Topic: somsat-transactions           |
   |                                           |                             |
   |                                           v                             |
   |                                  [ Speed Consumer ]                     |
   |                                  speed_consumer.py                      |
   |                                  - Feature Extraction                   |
   |                                  - Low-latency ML Inference (< 5ms)     |
   |                                  - Latency Benchmarking (P50/P95/P99)   |
   |                                           |                             |
   |                     +---------------------+---------------------+       |
   |                     |                                           |       |
   |                     v                                           v       |
   |            [ Alert Topic ]                               [ SQLite Live ]|
   |          somsat-fraud-alerts                             data/live_db   |
   +-----------------------------------------------------------------+-------+
                                                                     |
                                                                     v
                                                          +---------------------+
                                                          |    SERVING LAYER    |
                                                          |  Streamlit Dashboard|
                                                          | (http://localhost:8501)
                                                          +---------------------+
```

---

## 📊 2. SOMSAT Dataset Schema (Data Features)

SOMSAT dataset me realistic financial fraud patterns simulated hain:

| Feature Name | Type | Description | Normal Pattern | Fraud Pattern |
|--------------|------|-------------|----------------|---------------|
| `transaction_id` | String | Unique Transaction Identifier | `TXN_SOM_10001` | `TXN_SOM_10012` |
| `timestamp` | Datetime | Ingestion timestamp | Chronological | Bursts |
| `user_id` | String | Unique User ID | `USR_1001` to `USR_1400` | Target accounts |
| `amount` | Float | Transaction amount (₹) | ₹50 - ₹15,000 | ₹50,000 - ₹350,000 |
| `old_balance` / `new_balance` | Float | Sender balance delta | Moderate debit | Severe Account Drain (>90%) |
| `merchant_category` | String | Category of merchant | grocery, restaurant, utilities | crypto, luxury_retail, gaming |
| `channel` | String | Payment method | UPI, POS | NET_BANKING, ATM, UPI |
| `location_delta_km` | Float | Distance from previous txn (km) | 0 - 25 km | 500 - 3,000 km (Impossible Travel) |
| `device_trust_score` | Float | Device fingerprint integrity | 0.75 - 0.99 (Trusted) | 0.01 - 0.35 (Rooted/Emulator) |
| `velocity_1h` | Integer | Txn count in last 1 hour | 1 - 3 txns | 8 - 25 txns (Card Stuffing/Bot) |
| `is_fraud` | Binary | Ground Truth label | 0 | 1 |

---

## 📂 3. Project Directory Structure

```
somsat-kafka/
├── docker-compose.yml       # Apache Kafka broker (KRaft mode) + Kafka UI
├── requirements.txt         # Python dependencies (kafka-python-ng, scikit-learn, etc.)
├── run_pipeline.sh          # One-click execution bash script
├── README.md                # Complete documentation & viva guide
├── data/
│   ├── somsat.csv           # 6,000 realistic financial transaction records
│   ├── generate_dataset.py  # Dataset generator script
│   └── live_stream.db       # SQLite high-throughput live streaming buffer
├── models/
│   ├── model.pkl            # Trained Isolation Forest & StandardScaler pipeline
│   └── model_metadata.json  # Training metrics (ROC-AUC, Precision, Recall, F1)
├── batch_train.py           # Batch Layer: Trains Isolation Forest model
├── producer.py              # Speed Layer: Replays transactions to Kafka topic
├── speed_consumer.py        # Speed Layer: Consumes stream, runs inference, benchmarks latency
└── dashboard.py             # Serving Layer: Streamlit Real-Time Dashboard
```

---

## 🚀 4. Step-by-Step Setup & Execution Commands

### Step 1: Open Docker Desktop
Make sure **Docker Desktop** is open and running on your Mac/PC.

### Step 2: Navigate to Project Directory
```bash
cd somsat-kafka
```

### Step 3: Set up Python Virtual Environment (Optional if using global python)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Step 4: Start Kafka & Kafka UI via Docker
```bash
docker compose up -d
```
Verify containers are running:
```bash
docker compose ps
```
- **Kafka Broker:** `localhost:9092`
- **Kafka UI:** Open browser at `http://localhost:8080` to view cluster and topics.

### Step 5: Train the Batch Layer Model
```bash
python batch_train.py
```
*Output: Generates `models/model.pkl` with classification report and ROC-AUC score.*

### Step 6: Launch Streamlit Dashboard
Open a **new terminal tab** and run:
```bash
streamlit run dashboard.py
```
*Browser will open automatically at: `http://localhost:8501`*

### Step 7: Start Speed Layer Consumer
Open another **new terminal tab** and run:
```bash
python speed_consumer.py
```
*Consumer will connect to Kafka, load the model, and wait for transactions.*

### Step 8: Start Transaction Stream Producer
Open another **new terminal tab** and run:
```bash
python producer.py
```
*Producer will start streaming transactions into Kafka topic `somsat-transactions`.*

---

## 💡 5. Or Use the 1-Click Helper Script (`run_pipeline.sh`)

Instead of typing long commands, you can also use:
```bash
./run_pipeline.sh docker      # Starts Kafka Broker & Kafka UI in background
./run_pipeline.sh train       # Trains Isolation Forest model
./run_pipeline.sh dashboard   # Launches Streamlit UI (Terminal 1)
./run_pipeline.sh consumer    # Starts Consumer (Terminal 2)
./run_pipeline.sh producer    # Starts Streaming Producer (Terminal 3)
./run_pipeline.sh stop        # Stops Docker containers when done
```

---
