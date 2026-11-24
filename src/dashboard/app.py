import json
import time
import pandas as pd
import streamlit as st
from confluent_kafka import Consumer

st.set_page_config(page_title="UPI Real-Time Fraud Monitor", layout="wide")
st.title("UPI Real-Time Fraud Detection Monitor")

KAFKA_BOOTSTRAP = "localhost:19092"
ALERTS_TOPIC = "upi_fraud_alerts"

if "alerts" not in st.session_state:
    st.session_state.alerts = []


@st.cache_resource
def get_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "streamlit-fraud-dashboard",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([ALERTS_TOPIC])
    return consumer


consumer = get_consumer()

# Poll up to 20 new messages per refresh
for _ in range(20):
    msg = consumer.poll(0.1)
    if msg is None:
        break
    if msg.error():
        continue
    alert = json.loads(msg.value().decode("utf-8"))
    st.session_state.alerts.insert(0, alert)

# Keep latest 100 alerts in memory
st.session_state.alerts = st.session_state.alerts[:100]

df = pd.DataFrame(st.session_state.alerts)

# KPI Metrics Row
col1, col2, col3, col4 = st.columns(4)
total_alerts = len(df)
velocity_cnt = int((df["fraud_reason"] == "VELOCITY_BURST").sum()) if not df.empty else 0
high_val_cnt = int((df["fraud_reason"] == "HIGH_VALUE_ANOMALY").sum()) if not df.empty else 0
travel_cnt = int((df["fraud_reason"] == "IMPOSSIBLE_TRAVEL").sum()) if not df.empty else 0

col1.metric("Total Fraud Alerts", total_alerts)
col2.metric("Velocity Bursts", velocity_cnt)
col3.metric("High-Value Anomalies", high_val_cnt)
col4.metric("Impossible Travel", travel_cnt)

st.divider()

if not df.empty:
    st.subheader("Live Fraud Alerts Feed")
    display_cols = [
        "detected_at", "user_id", "fraud_reason",
        "txn_count", "total_amount", "max_amount", "distinct_cities"
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    st.dataframe(df[available_cols], use_container_width=True)

    st.subheader("Alerts by Fraud Type")
    st.bar_chart(df["fraud_reason"].value_counts())
else:
    st.info("Waiting for fraud alerts from PySpark (`upi_fraud_alerts` topic)...")

# Auto-refresh every 3 seconds
time.sleep(3)
st.rerun()