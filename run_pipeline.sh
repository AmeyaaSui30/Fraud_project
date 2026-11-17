#!/usr/bin/env bash
# ==============================================================================
# SOMSAT-KAFKA: Low-Latency Financial Fraud Detection Pipeline Launcher
# ==============================================================================

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR"

echo "======================================================================"
echo "🛡️  SOMSAT-KAFKA: Low-Latency Financial Fraud Detection"
echo "    Hybrid Streaming Architecture (Kafka + ML + Streamlit)"
echo "======================================================================"

# Check Docker status
if ! docker info >/dev/null 2>&1; then
    echo "❌ Error: Docker daemon is not running. Please start Docker Desktop first."
    exit 1
fi

# Detect Python environment
PYTHON_CMD="python3"
if [ -d ".venv" ]; then
    PYTHON_CMD="./.venv/bin/python"
    STREAMLIT_CMD="./.venv/bin/streamlit"
else
    STREAMLIT_CMD="streamlit"
fi

case "$1" in
    docker)
        echo "🚀 Starting Kafka and Kafka UI containers..."
        docker compose up -d
        echo "✅ Kafka is running on localhost:9092"
        echo "✅ Kafka UI is available at http://localhost:8080"
        ;;
    train)
        echo "🧠 Training Batch Layer Isolation Forest model..."
        $PYTHON_CMD batch_train.py
        ;;
    producer)
        echo "📤 Starting Transaction Stream Producer..."
        $PYTHON_CMD producer.py
        ;;
    consumer)
        echo "⚡ Starting Speed Layer Detection Consumer..."
        $PYTHON_CMD speed_consumer.py
        ;;
    dashboard)
        echo "📊 Launching Streamlit Real-Time Dashboard..."
        $STREAMLIT_CMD run dashboard.py
        ;;
    stop)
        echo "🛑 Stopping Docker containers..."
        docker compose down
        echo "✅ All containers stopped."
        ;;
    *)
        echo ""
        echo "Usage: ./run_pipeline.sh <command>"
        echo ""
        echo "Commands:"
        echo "  docker     - Start Kafka Broker and Kafka UI (Docker Compose)"
        echo "  train      - Train the Batch Layer model on SOMSAT dataset"
        echo "  consumer   - Start the Real-Time Speed Layer Consumer"
        echo "  producer   - Start streaming transactions to Kafka"
        echo "  dashboard  - Launch the Streamlit Monitoring Dashboard"
        echo "  stop       - Stop and remove Docker containers"
        echo ""
        echo "Recommended Quickstart Order:"
        echo "  1) ./run_pipeline.sh docker"
        echo "  2) ./run_pipeline.sh train"
        echo "  3) ./run_pipeline.sh dashboard   (Terminal 1)"
        echo "  4) ./run_pipeline.sh consumer    (Terminal 2)"
        echo "  5) ./run_pipeline.sh producer    (Terminal 3)"
        echo ""
        ;;
esac
