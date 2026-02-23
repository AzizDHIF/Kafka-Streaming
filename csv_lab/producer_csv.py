#Write your csv producer code here

import time
import csv
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'transactions-csv'
FILE = 'data/transactions_dirty.csv'

with open(FILE, 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    print(f"Header: {header}")

    for i, row in enumerate(reader):
        message = ','.join(row)
        producer.send(TOPIC, value=message.encode('utf-8'))
        print(f"[{i}] Sent: {message}")
        time.sleep(0.5)

producer.flush()
print("All messages sent.")