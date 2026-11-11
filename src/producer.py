import json
import time
import polars as pl
from kafka import KafkaProducer

print("🚀 Starting Controlled Kafka Producer (1 txn / 1.0 sec)...")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'credit-card-transactions'
PARQUET_PATH = 'test_us_fraud_100k.parquet'

df = pl.read_parquet(PARQUET_PATH)
records = df.to_dicts()

print(f"📦 Loaded {len(records):,} records. Streaming started at 1 txn/sec...")

count = 0
for record in records:
    record['timestamp'] = str(record['timestamp'])
    producer.send(TOPIC_NAME, value=record)
    producer.flush()
    count += 1
    
    print(f"📡 Sent #{count} | Txn: {record['transaction_id']} | Amount: ${record['amount']} | State: {record['state']}")
    
    # ⏱️ Dəqiq 1 saniyədən bir 1 sətir göndərmək üçün
    time.sleep(1.0)

print("✅ Streaming completed!")