import json
import os
import re
from kafka import KafkaConsumer

TOPIC      = 'transactions-json-dirty'
GROUP_ID   = 'json-group-dirty-1'
OUTPUT_DIR = f'output_json_{TOPIC}'
os.makedirs(OUTPUT_DIR, exist_ok=True)

TIMESTAMP_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=3000,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print(f"Listening on '{TOPIC}' as group '{GROUP_ID}'...")
print("-" * 60)

message_count = 0
valid_count   = 0
invalid_count = 0
total_amount  = 0.0
batch_number  = 1
batch_buffer  = []

for message in consumer:
    message_count += 1
    raw   = message.value
    issue = None

    try:
        if not isinstance(raw, dict):
            raise ValueError("message is not a valid JSON object")

        transaction_id = raw.get('transaction_id')
        user_id        = raw.get('user_id')
        amount         = raw.get('amount')
        timestamp      = raw.get('timestamp')

        issues = []

        # --- Check missing fields ---
        missing = [f for f, v in {
            'transaction_id': transaction_id,
            'user_id':        user_id,
            'amount':         amount,
            'timestamp':      timestamp
        }.items() if v is None]
        if missing:
            issues.append(f"missing fields: {missing}")

        # --- Check user_id is numeric ---
        if user_id is not None and not str(user_id).isdigit():
            issues.append(f"non-numeric user_id: '{user_id}'")

        # --- Check amount is numeric ---
        if amount is not None and not isinstance(amount, (int, float)):
            issues.append(f"non-numeric amount: '{amount}'")

        # --- Check timestamp not empty ---
        if timestamp == '':
            issues.append("empty timestamp")
        elif timestamp is not None and not TIMESTAMP_PATTERN.match(str(timestamp)):
            issues.append(f"invalid timestamp format: '{timestamp}'")

        if issues:
            issue = ' | '.join(issues)
            raise ValueError(issue)

        # --- All good ---
        valid_count  += 1
        total_amount += amount
        status        = "OK     "

        print(
            f"[{status}] Partition={message.partition} | "
            f"Offset={message.offset} | "
            f"ID={transaction_id} | "
            f"User={user_id} | "
            f"Amount={amount} | "
            f"Timestamp={timestamp}"
        )

    except (ValueError, AttributeError) as e:
        invalid_count += 1
        status         = "INVALID"
        issue          = str(e)

        print(
            f"[{status}] Partition={message.partition} | "
            f"Offset={message.offset} | "
            f"Raw={raw}"
        )
        print(f"           WARNING: {issue}")

    print(f"           Total={message_count} | Valid={valid_count} | Invalid={invalid_count} | Total Amount={round(total_amount, 2)}")
    print()

    # --- Build line for batch file ---
    line = (
        f"[{status}] Partition={message.partition} | "
        f"Offset={message.offset} | "
        f"ID={raw.get('transaction_id') if isinstance(raw, dict) else 'N/A'} | "
        f"User={raw.get('user_id') if isinstance(raw, dict) else 'N/A'} | "
        f"Amount={raw.get('amount') if isinstance(raw, dict) else 'N/A'} | "
        f"Timestamp={raw.get('timestamp') if isinstance(raw, dict) else 'N/A'} | "
        f"Issues: {issue if issue else 'none'}\n"
    )
    batch_buffer.append(line)

    # --- Save every 10 events ---
    if len(batch_buffer) == 10:
        filename = os.path.join(OUTPUT_DIR, f"batch_{batch_number}.txt")
        with open(filename, 'w') as f:
            f.write(f"=== Batch {batch_number} | Group: {GROUP_ID} ===\n")
            f.write("-" * 60 + "\n")
            f.writelines(batch_buffer)
            f.write("-" * 60 + "\n")
            f.write(f"Total={message_count} | Valid={valid_count} | Invalid={invalid_count} | Total Amount={round(total_amount, 2)}\n")

        print(f"  >>> Saved batch_{batch_number}.txt\n")
        batch_buffer = []
        batch_number += 1

# --- Save remaining messages ---
if batch_buffer:
    filename = os.path.join(OUTPUT_DIR, f"batch_{batch_number}.txt")
    with open(filename, 'w') as f:
        f.write(f"=== Batch {batch_number} | Group: {GROUP_ID} (final) ===\n")
        f.write("-" * 60 + "\n")
        f.writelines(batch_buffer)
        f.write("-" * 60 + "\n")
        f.write(f"Total={message_count} | Valid={valid_count} | Invalid={invalid_count} | Total Amount={round(total_amount, 2)}\n")

    print(f"  >>> Saved final batch_{batch_number}.txt ({len(batch_buffer)} remaining messages)\n")

# --- Final summary ---
print("=" * 60)
print("FINAL SUMMARY")
print(f"  Total messages : {message_count}")
print(f"  Valid          : {valid_count}")
print(f"  Invalid        : {invalid_count}")
print(f"  Total Amount   : {round(total_amount, 2)}")
print("=" * 60)