from kafka import KafkaProducer
import pandas as pd

# Konfigurasi Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: str(v).encode("utf-8")
)

# Membaca file CSV
data = pd.read_csv("../data/Processed Property Sales.csv")

# Mengirim data ke Kafka
for _, row in data.iterrows():
    producer.send("property-sales", value=row.to_dict())
    print(f"Sent: {row.to_dict()}")

producer.flush()
print("Data sent to Kafka.")
