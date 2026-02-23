from kafka import KafkaConsumer
import os
import re

TOPIC    = 'transactions-csv'
GROUP_ID = 'csv-group-dirty-3'
OUTPUT_DIR = 'output_dirty'
os.makedirs(OUTPUT_DIR, exist_ok=True)

EXPECTED_COLUMNS = 4  # transaction_id, user_id, amount, timestamp
TIMESTAMP_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')  # 2024-01-01 10:00:05

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    group_id=GROUP_ID,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=3000
)

print(f"Listening on '{TOPIC}' as group '{GROUP_ID}'...")
print("-" * 60)

message_count = 0
valid_count   = 0
invalid_count = 0
batch_number  = 1
batch_buffer  = []
seen_rows     = set()  # for duplicate detection

for message in consumer:
    raw    = message.value.decode('utf-8').strip()
    fields = raw.split(',')

    message_count += 1
    issues = []

    # --- Issue 1: Wrong number of columns ---
    if len(fields) != EXPECTED_COLUMNS:
        issues.append(
            f"wrong column count: expected {EXPECTED_COLUMNS}, got {len(fields)}"
        )

    # only check field content if we have enough fields
    if len(fields) == EXPECTED_COLUMNS:
        transaction_id = fields[0].strip()
        user_id        = fields[1].strip()
        amount         = fields[2].strip()
        timestamp      = fields[3].strip()

        # --- Issue 2: user_id must be numeric ---
        if not user_id.isdigit():
            issues.append(f"non-numeric user_id: '{user_id}'")

        # --- Issue 3: amount must be a valid float and not empty ---
        if amount == '':
            issues.append("empty amount")
        else:
            try:
                float(amount)
            except ValueError:
                issues.append(f"non-numeric amount: '{amount}'")

        # --- Issue 4: timestamp empty ---
        if timestamp == '':
            issues.append("empty timestamp")

        # --- Issue 5: timestamp wrong format ---
        elif not TIMESTAMP_PATTERN.match(timestamp):
            issues.append(f"invalid timestamp format: '{timestamp}'")

    # --- Issue 6: duplicate row ---
    if raw in seen_rows:
        issues.append("duplicate row")
    else:
        seen_rows.add(raw)

    # --- Status ---
    if issues:
        invalid_count += 1
        status = "INVALID"
    else:
        valid_count += 1
        status = "OK    "

    # --- Print to console ---
    print(
        f"[{status}] Partition={message.partition} | "
        f"Offset={message.offset} | "
        f"Value={raw}"
    )
    if issues:
        for issue in issues:
            print(f"         WARNING: {issue}")

    print(f"         Total={message_count} | Valid={valid_count} | Invalid={invalid_count}")
    print()

    # --- Build line for batch file ---
    issue_str = ' | '.join(issues) if issues else 'none'
    line = (
        f"[{status}] Partition={message.partition} | "
        f"Offset={message.offset} | "
        f"Value={raw}\n"
        f"         Issues: {issue_str}\n"
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
            f.write(f"Total={message_count} | Valid={valid_count} | Invalid={invalid_count}\n")

        print(f"  >>> Saved batch_{batch_number}.txt\n")
        batch_buffer = []
        batch_number += 1

# --- Save remaining messages that didn't fill a complete batch ---
if batch_buffer:
    filename = os.path.join(OUTPUT_DIR, f"batch_{batch_number}.txt")
    with open(filename, 'w') as f:
        f.write(f"=== Batch {batch_number} | Group: {GROUP_ID} (final) ===\n")
        f.write("-" * 60 + "\n")
        f.writelines(batch_buffer)
        f.write("-" * 60 + "\n")
        f.write(f"Total={message_count} | Valid={valid_count} | Invalid={invalid_count}\n")

    print(f"  >>> Saved final batch_{batch_number}.txt ({len(batch_buffer)} remaining messages)\n")