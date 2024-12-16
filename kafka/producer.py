from confluent_kafka import Producer
import pandas as pd

# Konfigurasi Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9092',  # Alamat Kafka broker
}

producer = Producer(producer_config)

# Callback untuk delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Membaca file CSV
data = pd.read_csv("../data/Processed Property Sales.csv")

# Mengirim data ke Kafka
for _, row in data.iterrows():
    value = row.to_dict()  # Data dalam format dictionary
    producer.produce(
        'property-sales',
        key=str(row.get('UniqueID', 'default_key')),  # Ganti 'UniqueID' dengan kolom kunci Anda
        value=str(value),  # Convert dictionary ke string
        callback=delivery_report
    )

# Pastikan semua pesan terkirim
producer.flush()
print("All messages have been sent.")

