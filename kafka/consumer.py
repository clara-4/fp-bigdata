from kafka import KafkaConsumer
import pandas as pd
import boto3
import io

# Konfigurasi Kafka
consumer = KafkaConsumer(
    "property-sales",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: eval(v.decode("utf-8"))
)

# Konfigurasi MinIO
minio_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="dua",
    aws_secret_access_key="kelompok2"
)

bucket_name = "property-data"
data = []

for message in consumer:
    data.append(message.value)
    print(f"Received: {message.value}")

    if len(data) >= 100:  # Batch size
        df = pd.DataFrame(data)
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
