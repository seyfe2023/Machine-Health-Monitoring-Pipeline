import json
from kafka import KafkaConsumer
import psycopg2

# Kafka config
consumer = KafkaConsumer(
    'machine_readings',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# DB connection
conn = psycopg2.connect(
    host="localhost",
    database="machine_db",
    user="admin",
    password="admin"
)
cursor = conn.cursor()

print("Consumer started...")

for message in consumer:
    data = message.value

    machine_id = data["machine_id"]
    timestamp = data["timestamp"]
    temperature = data["temperature"]
    vibration = data["vibration"]
    pressure = data["pressure"]

    # 🔥 REAL-TIME anomaly detection
    if temperature > 90:
        print(f"🚨 ALERT: High temperature! Machine {machine_id} = {temperature}")

    # Insert into TimescaleDB
    insert_query = """
        INSERT INTO machine_readings 
        (machine_id, timestamp, temperature, vibration, pressure)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (machine_id, timestamp) DO NOTHING;
    """

    cursor.execute(insert_query, (
        machine_id,
        timestamp,
        temperature,
        vibration,
        pressure
    ))

    conn.commit()

    print("Inserted:", data)
