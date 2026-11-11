import time
import psycopg2
import pandas as pd
import plotly.express as px
import streamlit as st

# Page Configuration
st.set_page_config(
    page_title="Real-Time Credit Card Fraud Monitor",
    page_icon="💳",
    layout="wide"
)

st.title("💳 Real-Time Credit Card Fraud Detection Dashboard")
st.caption("Powered by Apache Kafka, LightGBM Inference Engine & OCI PostgreSQL")

# Database Fetcher Function (Fetches ALL records live)
def fetch_live_data():
    try:
        conn = psycopg2.connect(
            dbname="fraud_detection_db",
            user="postgres",
            password="your_secure_password",
            host="130.61.180.68",
            port="5432",
            connect_timeout=5
        )
        # Baza üzrə bütün məlumatları oxuyuruq
        query = """
        SELECT 
            transaction_id, timestamp, customer_id, card_id, amount, state,
            merchant_name, merchant_category, device_type, network_type,
            actual_is_fraud, predicted_is_fraud, predicted_fraud_probability, processed_at
        FROM real_time_fraud_predictions
        ORDER BY processed_at DESC;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

# Data Pipeline Exec
df = fetch_live_data()

if not df.empty and 'predicted_is_fraud' in df.columns:
    # ÜMUMİ (TOTAL) STATİSTİKALAR — Bütün Baza Üzrə
    total_txns = len(df)
    fraud_df = df[df['predicted_is_fraud'] == 1]
    total_fraud_txns = len(fraud_df)
    total_fraud_rate = (total_fraud_txns / total_txns) * 100 if total_txns > 0 else 0
    total_fraud_amount = fraud_df['amount'].sum() if not fraud_df.empty else 0.0

    # 1. Metric KPIs (Baza üzrə Tam Ümumi Göstəricilər)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total DB Transactions", f"{total_txns:,}")
    col2.metric("Total Detected Frauds", f"{total_fraud_txns:,}", delta=f"{total_fraud_rate:.2f}% Rate", delta_color="inverse")
    col3.metric("Total Blocked Amount", f"${total_fraud_amount:,.2f}")
    col4.metric("Engine Health", "🟢 Live Streaming", delta="Kafka & LightGBM Active")
    st.divider()

    # 2. Charts and Maps (Bütün Bazadakı Saxtakarlıqlar Üzrə)
    col_map, col_pie = st.columns([2, 1])
    
    # US State Map Fraud Density (Ümumi Baza Üzrə)
    if not fraud_df.empty and 'state' in fraud_df.columns:
        state_fraud = fraud_df.groupby('state').size().reset_index(name='fraud_count')
        fig_map = px.choropleth(
            state_fraud,
            locations='state',
            locationmode="USA-states",
            color='fraud_count',
            scope="usa",
            color_continuous_scale="Reds",
            title="🚨 Total Fraud Density by US State"
        )
        fig_map.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
        col_map.plotly_chart(fig_map, use_container_width=True)

    # Prediction Distribution (Ümumi Risk Paylanması)
    status_counts = df['predicted_is_fraud'].map({0: 'Normal', 1: 'Fraud'}).value_counts().reset_index()
    status_counts.columns = ['Status', 'Count']
    fig_pie = px.pie(
        status_counts, 
        names='Status', 
        values='Count',
        color='Status',
        color_discrete_map={'Normal':'#2ecc71', 'Fraud':'#e74c3c'},
        title="Overall Risk Breakdown"
    )
    col_pie.plotly_chart(fig_pie, use_container_width=True)

    # 3. Live Feed Table (Ən son daxil olan 10 tranzaksiya)
    st.markdown("### 🚨 Live Feed & High Risk Alerts (Latest 10 Transactions)")
    display_cols = ['transaction_id', 'timestamp', 'amount', 'state', 'merchant_category', 'predicted_fraud_probability', 'predicted_is_fraud']
    display_df = df.head(10)[display_cols].copy()
    display_df['Status'] = display_df['predicted_is_fraud'].apply(lambda x: "🚨 FRAUD DETECTED" if x == 1 else "✅ NORMAL")
    display_df['Risk Probability'] = display_df['predicted_fraud_probability'].apply(lambda x: f"{x:.4f}")
    
    final_table = display_df[['transaction_id', 'timestamp', 'amount', 'state', 'merchant_category', 'Risk Probability', 'Status']]

    def highlight_fraud(row):
        return ['background-color: #ffcccc' if row['Status'] == "🚨 FRAUD DETECTED" else '' for _ in row]

    st.dataframe(
        final_table.style.apply(highlight_fraud, axis=1),
        use_container_width=True
    )
else:
    st.info("⏳ Connecting to OCI PostgreSQL live stream...")

# Auto-Refresh loop (Every 2 seconds)
time.sleep(2)
st.rerun()