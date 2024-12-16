import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
import pickle
import boto3
import io
import os
import time
from sklearn.preprocessing import LabelEncoder  # Import LabelEncoder

# Konfigurasi MinIO
minio_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="dua",
    aws_secret_access_key="kelompok2"
)

bucket_name = "property-data"
processed_batches_file = "processed_batches.txt"

def load_processed_batches():
    """Memuat daftar batch yang sudah diproses."""
    if os.path.exists(processed_batches_file):
        with open(processed_batches_file, 'r') as file:
            return set(file.read().splitlines())
    return set()

def save_processed_batches(processed_batches):
    """Menyimpan daftar batch yang sudah diproses."""
    with open(processed_batches_file, 'w') as file:
        file.write('\n'.join(processed_batches))

def download_batches_from_minio(processed_batches):
    """Mengunduh batch baru dari MinIO dan menggabungkannya menjadi satu DataFrame."""
    objects = minio_client.list_objects(Bucket=bucket_name)
    all_data = []
    new_batches = []

    for obj in objects.get('Contents', []):  # Pastikan objeknya ada dalam 'Contents'
        if obj['Key'].startswith("batch_") and obj['Key'].endswith(".csv") and obj['Key'] not in processed_batches:
            response = minio_client.get_object(Bucket=bucket_name, Key=obj['Key'])
            batch_data = pd.read_csv(io.BytesIO(response['Body'].read()))

            # Mengonversi kolom 'type' menjadi 'type_indexed' yang berisi angka
            le = LabelEncoder()
            batch_data['Type_indexed'] = le.fit_transform(batch_data['Type'])

            all_data.append(batch_data)
            new_batches.append(obj['Key'])

    if all_data:
        return pd.concat(all_data, ignore_index=True), new_batches
    else:
        return pd.DataFrame(), []

def train_model():
    """Melatih model menggunakan data batch baru dari MinIO."""
    processed_batches = load_processed_batches()
    data, new_batches = download_batches_from_minio(processed_batches)

    if data.empty:
        print("Tidak ada batch baru untuk training.")
        return

    # Memilih fitur dan target
    X = data[['Rooms', 'Type_indexed', 'Distance', 'BuildingArea']]
    y = data['Price']

    # Membagi data menjadi training dan testing
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Model Random Forest
    rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
    rf_model.fit(X_train, y_train)

    # Evaluasi Random Forest
    y_pred = rf_model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = rf_model.score(X_test, y_test)  # Menghitung R²

    # Menghitung Adjusted R²
    n = X_test.shape[0]  # Jumlah sampel
    p = X_test.shape[1]  # Jumlah fitur
    adjusted_r2 = 1 - ((1 - r2) * (n - 1) / (n - p - 1))

    print(f"Random Forest MSE: {mse}")
    print(f"Random Forest R²: {r2}")
    print(f"Random Forest Adjusted R²: {adjusted_r2}")

    # Simpan model Random Forest sebagai .pkl
    rf_model_filename = 'random_forest_model.pkl'
    with open(rf_model_filename, 'wb') as file:
        pickle.dump(rf_model, file)

    print("Model Random Forest telah dilatih dan disimpan.")

    # Perbarui daftar batch yang telah diproses
    processed_batches.update(new_batches)
    save_processed_batches(processed_batches)
    print("Daftar batch yang telah diproses diperbarui.")

# Looping untuk memeriksa data baru setiap 10 detik
while True:
    train_model()
    print("Menunggu batch baru...")
    time.sleep(10)  # Jeda selama 10 detik sebelum memeriksa lagi
