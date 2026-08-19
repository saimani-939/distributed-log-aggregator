import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

TOPIC_NAME = 'app-logs'
SERVICES = ['auth-service', 'payment-gateway', 'order-service', 'inventory-service', 'notification-service']
LOG_LEVELS = ['INFO', 'INFO', 'INFO', 'WARN', 'ERROR']  # Weighted towards normal traffic
ERROR_MESSAGES = [
    'Database connection timeout during checkout',
    'Third-party payment gateway rejected payload',
    'Out of memory exception during batch sync',
    'JWT token validation expired',
    'Redis cache miss on session token'
]

def generate_log():
    service = random.choice(SERVICES)
    level = random.choice(LOG_LEVELS)
    status_code = 200

    if level == 'INFO':
        message = f"Successfully processed request for user_{random.randint(100, 999)}"
        status_code = random.choice([200, 201])
    elif level == 'WARN':
        message = f"High latency detected: {random.randint(400, 1200)}ms"
        status_code = random.choice([400, 404, 429])
    else:  # ERROR
        message = random.choice(ERROR_MESSAGES)
        status_code = 500

    return {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "service": service,
        "level": level,
        "status_code": status_code,
        "message": message,
        "trace_id": str(uuid.uuid4())
    }

print("🚀 Starting real-time microservice log generator... (Press Ctrl+C to stop)")

try:
    while True:
        log_data = generate_log()
        # Push message to Kafka topic
        producer.send(
            topic=TOPIC_NAME,
            key=log_data['service'],
            value=log_data
        )
        print(f"[{log_data['level']}] {log_data['service']} (Code: {log_data['status_code']}): {log_data['message']}")
        time.sleep(0.1)  # Streams ~10 logs per second
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()
    producer.close()