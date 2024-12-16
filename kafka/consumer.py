from confluent_kafka import Consumer
import pandas as pd
import boto3
import io

# Konfigurasi Kafka
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Alamat Kafka broker
    'group.id': 'property-sales-group',     # ID grup consumer
    'auto.offset.reset': 'earliest',        # Mulai membaca dari offset awal
}

consumer = Consumer(consumer_config)

# Subscribe ke topik
consumer.subscribe(['property-sales'])

# Konfigurasi MinIO
minio_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="dua",
    aws_secret_access_key="kelompok2"
)

bucket_name = "property-data"
data = []

try:
    while True:
        # Poll pesan dari Kafka
        msg = consumer.poll(1.0)  # Timeout 1 detik
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Decode pesan
        value = msg.value().decode('utf-8')
        print(f"Received: {value}")
        data.append(value)

        # Simpan data ke MinIO setiap batch
        if len(data) >= 100:  # Batch size
            df = pd.DataFrame([eval(item) for item in data])  # Convert string to dictionary
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            minio_client.put_object(
                Bucket=bucket_name,
                Key="processed_data.csv",
                Body=csv_buffer.getvalue(),
                ContentType="text/csv"
            )
            print("Data batch saved to MinIO.")
            data = []  # Clear buffer

except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
