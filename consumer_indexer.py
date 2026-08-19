import json
import time
from datetime import datetime
from kafka import KafkaConsumer
import clickhouse_connect

# 1. Connect to ClickHouse
client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
print("✅ Connected to ClickHouse")

# 2. Connect to Kafka Consumer
consumer = KafkaConsumer(
    'app-logs',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='log-indexer-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

BATCH_SIZE = 50
FLUSH_INTERVAL = 2.0  # seconds

buffer = []
last_flush_time = time.time()

print("📥 Starting ClickHouse Log Indexer Consumer... Listening for events...")

def flush_to_clickhouse():
    global buffer, last_flush_time
    if not buffer:
        return
    
    try:
        # Batch insert rows into ClickHouse
        client.insert(
            'logs',
            buffer,
            column_names=['timestamp', 'service', 'level', 'status_code', 'message', 'trace_id']
        )
        print(f"⚡ Flushed {len(buffer)} logs to ClickHouse at {datetime.now().strftime('%H:%M:%S')}")
        buffer = []
        last_flush_time = time.time()
    except Exception as e:
        print(f"❌ Error inserting to ClickHouse: {e}")

try:
    for message in consumer:
        log = message.value
        
        # Format row
        row = (
            datetime.strptime(log['timestamp'], "%Y-%m-%d %H:%M:%S"),
            log['service'],
            log['level'],
            int(log['status_code']),
            log['message'],
            log['trace_id']
        )
        buffer.append(row)

        # Flush condition: reached batch size OR time elapsed
        if len(buffer) >= BATCH_SIZE or (time.time() - last_flush_time) >= FLUSH_INTERVAL:
            flush_to_clickhouse()

except KeyboardInterrupt:
    print("\nStopping consumer...")
    flush_to_clickhouse()
finally:
    consumer.close()