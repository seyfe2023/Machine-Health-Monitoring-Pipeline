import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "machine_readings"

def generate_temperature():
    # 10% anomaly chance
    if random.random() < 0.1:
        return random.randint(90, 110)
    return random.randint(60, 85)

def generate_data():
    return {
        "machine_id": f"M{random.randint(1,5)}",
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": generate_temperature(),
        "vibration": round(random.uniform(0.1, 1.0), 2),
        "pressure": round(random.uniform(1.0, 5.0), 2)
    }

print("Producer started...")

while True:
    data = generate_data()
    print("Sending:", data)

    producer.send(TOPIC, value=data)
    producer.flush()

    time.sleep(2)
