from confluent_kafka import Producer
import pandas as pd
import json
import time

producer_config = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

data = pd.read_csv("../data/Property Sales of Melbourne City.csv")

for _, row in data.iterrows():
    value = json.dumps(row.to_dict())  # Serialize dictionary ke JSON string
    key = str(row.get('UniqueID', 'default_key'))  # Gunakan kolom kunci
    producer.produce(
        'property-sales',
        key=key,
        value=value,
        callback=delivery_report
    )
    print(f"Sending data: {value}")
    time.sleep(0.3)

producer.flush()
print("All messages have been sent.")
