import time
import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = 'transactions-json-dirty'
FILE  = r'../transactions_dirty.csv'

print(f"Sending to topic '{TOPIC}'...")
print("-" * 60)

with open(FILE, 'r') as f:
    reader = csv.DictReader(f)

    for i, row in enumerate(reader):
        try:
            event = {
                "transaction_id": int(row.get("transaction_id", "").strip()),
                "user_id":        row.get("user_id", "").strip(),  # keep as string — 'abc' is possible
                "amount":         float(row.get("amount", "").strip()),  # raises ValueError if empty or 'seven'
                "timestamp":      row.get("timestamp", "").strip(),
            }
            producer.send(TOPIC, value=event)
            print(f"[{i}] Sent:    {event}")
        except (ValueError, KeyError) as e:
            print(f"[{i}] SKIPPED: {dict(row)} -- reason: {e}")

        time.sleep(0.5)

producer.flush()
print("-" * 60)
print("Done.")