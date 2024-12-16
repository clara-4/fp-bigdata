from confluent_kafka import Consumer
import pandas as pd
import json
import boto3
import io

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'property-sales',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(consumer_config)
consumer.subscribe(['property-sales'])

# MinIO configuration
minio_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="dua",
    aws_secret_access_key="kelompok2"
)

bucket_name = "property-data"
data = []
batch_counter = 1  # Counter for batch numbers

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        value = msg.value().decode('utf-8')

        try:
            parsed_value = json.loads(value)
        except json.JSONDecodeError as e:
            print(f"Error parsing value: {e}")
            continue

        data.append(parsed_value)

        if len(data) >= 100:  # Process batch when it reaches size 100
            print(f"Batch {batch_counter} diterima.")

            # Create a DataFrame from the batch data
            new_df = pd.DataFrame(data)

            # Save the batch to a new file in MinIO
            batch_file_name = f"batch_{batch_counter}.csv"
            csv_buffer = io.StringIO()
            new_df.to_csv(csv_buffer, index=False)
            minio_client.put_object(
                Bucket=bucket_name,
                Key=batch_file_name,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv"
            )

            print(f"Batch {batch_counter} saved as {batch_file_name} to MinIO.")
            batch_counter += 1
            data = []  # Clear the batch data after saving

except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
