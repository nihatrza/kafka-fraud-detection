import json
import joblib
import psycopg2
import pandas as pd
from kafka import KafkaConsumer

print("🚀 Starting Kafka Consumer with LightGBM Inference Engine...")

# 1. Model Və Encoder-lərin Yüklənməsi
artifact = joblib.load('best_fraud_model.pkl')
model = artifact['model']
label_encoders = artifact['label_encoders']
feature_names = artifact['feature_names']

print(f"✅ Loaded Model: {artifact['model_name']}")

# 2. PostgreSQL Bazasına Bağlantı
conn = psycopg2.connect(
    dbname="fraud_detection_db",
    user="postgres",
    password="your_secure_password",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# 3. Kafka Consumer Setup
consumer = KafkaConsumer(
    'credit-card-transactions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

INSERT_QUERY = """
INSERT INTO real_time_fraud_predictions (
    transaction_id, timestamp, customer_id, card_id, amount, currency, state,
    merchant_name, merchant_category, device_type, network_type, device_trust_score,
    is_new_device, hour_of_day, day_of_week, is_weekend, is_night_txn, log_amount,
    is_high_value_txn, is_high_risk_merchant, is_anonymous_network, card_median_amount,
    pca_risk_factor_1, pca_risk_factor_2, ratio_to_card_median, rule_risk_score,
    actual_is_fraud, actual_fraud_type, predicted_is_fraud, predicted_fraud_probability
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING;
"""

processed_count = 0

print("🎧 Listening to 'credit-card-transactions' topic...")

for message in consumer:
    record = message.value
    input_data = pd.DataFrame([record])
    
    # Categorical sütunları LabelEncoder vasitəsilə kodlaşdırırıq
    for col, le in label_encoders.items():
        if col in input_data.columns:
            val = str(input_data[col].values[0])
            input_data[col] = le.transform([val]) if val in le.classes_ else 0

    # Modelin gözlədiyi dəqiq sütun sırasını seçirik
    X_input = input_data[feature_names]
    
    # Real-Time ML Təxmini
    pred_class = int(model.predict(X_input)[0])
    pred_prob = float(model.predict_proba(X_input)[0][1])
    
    # PostgreSQL üçün dəyərlər
    db_values = (
        record['transaction_id'], record['timestamp'], record['customer_id'], record['card_id'],
        record['amount'], record['currency'], record['state'], record['merchant_name'],
        record['merchant_category'], record['device_type'], record['network_type'],
        record['device_trust_score'], record['is_new_device'], record['hour_of_day'],
        record['day_of_week'], record['is_weekend'], record['is_night_txn'], record['log_amount'],
        record['is_high_value_txn'], record['is_high_risk_merchant'], record['is_anonymous_network'],
        record['card_median_amount'], record['pca_risk_factor_1'], record['pca_risk_factor_2'],
        record['ratio_to_card_median'], record['risk_score'], record['is_fraud'],
        record['fraud_type'], pred_class, pred_prob
    )
    
    cursor.execute(INSERT_QUERY, db_values)
    conn.commit()
    
    processed_count += 1
    alert_flag = "🚨 FRAUD DETECTED!" if pred_class == 1 else "✅ NORMAL"
    print(f"📥 [{processed_count}] Txn: {record['transaction_id']} | Risk Prob: {pred_prob:.4f} | Status: {alert_flag}")