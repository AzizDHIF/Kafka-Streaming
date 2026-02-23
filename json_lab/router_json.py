import json
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'transactions-json',
    bootstrap_servers='localhost:9092',
    group_id='json-router',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Router running...")
print("-" * 60)

for message in consumer:
    event  = message.value
    amount = event.get('amount', 0)

    if amount > 100:
        producer.send('transactions-filtered', value=event)
        print(f"HIGH VALUE  → transactions-filtered | Amount={amount} | ID={event.get('transaction_id')}")
    else:
        producer.send('transactions-raw', value=event)
        print(f"NORMAL      → transactions-raw      | Amount={amount} | ID={event.get('transaction_id')}")