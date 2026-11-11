import json
import os
import pandas as pd
import lightgbm as lgb
from kafka import KafkaConsumer
from config.db_config import get_db_connection

# Load trained LightGBM model
MODEL_PATH = os.path.join("models", "lightgbm_fraud_model.pkl")
model = lgb.Booster(model_file=MODEL_PATH)

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    'financial_transactions',
    bootstrap_servers=[os.getenv("KAFKA_BROKER", "localhost:9092")],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = get_db_connection()
cursor = conn.cursor()

batch_count = 0
BATCH_SIZE = 50

print("[INFO] Kafka Consumer listening for real-time transactions...")

insert_query = """
INSERT INTO real_time_fraud_predictions (
    transaction_id, amount, merchant_category, state, 
    device_type, network_type, predicted_is_fraud, 
    predicted_fraud_probability
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING;
"""

try:
    for message in consumer:
        txn = message.value
        
        # Prepare feature vector for LightGBM inference
        # (Extract raw features required by model)
        features = pd.DataFrame([txn['features']])
        
        # Predict fraud probability
        prob = model.predict(features)[0]
        is_fraud = 1 if prob >= 0.80 else 0
        
        # Execute insert
        cursor.execute(insert_query, (
            txn['transaction_id'],
            txn['amount'],
            txn['merchant_category'],
            txn['state'],
            txn['device_type'],
            txn['network_type'],
            is_fraud,
            round(float(prob), 4)
        ))
        
        batch_count += 1
        
        # Batch Commit for Optimized DB Operations
        if batch_count % BATCH_SIZE == 0:
            conn.commit()
            print(f"[INFO] Processed & Committed {batch_count} transaction records.")

except KeyboardInterrupt:
    conn.commit()
    cursor.close()
    conn.close()
    print("[INFO] Consumer stopped gracefully.")
