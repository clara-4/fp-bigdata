import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import pickle
import boto3
import io
import os
from sklearn.preprocessing import LabelEncoder
import streamlit as st

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
        return pd.concat(all_data, ignore_index=True), new_batches, le
    else:
        return pd.DataFrame(), [], None

def train_model():
    """Melatih model menggunakan data batch baru dari MinIO."""
    processed_batches = load_processed_batches()
    data, new_batches, le = download_batches_from_minio(processed_batches)

    if data.empty:
        st.write("Tidak ada batch baru untuk training.")
        return None, 0, None, None, None

    # Memilih fitur dan target
    X = data[['Rooms', 'Type_indexed', 'Distance', 'BuildingArea']]
    y = data['Price']

    # Membagi data menjadi training dan testing
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Model Random Forest
    rf_model = RandomForestRegressor(n_estimators=100, random_state=42)
    rf_model.fit(X_train, y_train)

    # Prediksi pada data testing
    y_pred = rf_model.predict(X_test)

    # Simpan model Random Forest sebagai .pkl
    rf_model_filename = 'random_forest_model.pkl'
    with open(rf_model_filename, 'wb') as file:
        pickle.dump(rf_model, file)

    # Perbarui daftar batch yang telah diproses
    processed_batches.update(new_batches)
    save_processed_batches(processed_batches)

    return y_pred, len(new_batches), y_test.reset_index(drop=True), data[['Type', 'Type_indexed']].drop_duplicates(), le

# Streamlit UI
st.title("Visualisasi Prediksi Harga Properti")
st.write("Aplikasi ini melatih model menggunakan data dari MinIO dan menampilkan prediksi harga.")

# Latih model dan dapatkan hasil prediksi
predicted_prices, batch_count, actual_prices, type_data, label_encoder = train_model()

if predicted_prices is not None:
    # Tampilkan jumlah batch yang diproses
    st.subheader("Jumlah Batch Baru yang Diproses")
    st.write(f"{batch_count} batch baru berhasil diproses.")

    # Membuat DataFrame untuk harga aktual dan prediksi
    prices_df = pd.DataFrame({
        "Harga Aktual": actual_prices,
        "Harga Prediksi": predicted_prices
    })

    # Tampilkan DataFrame
    st.subheader("Harga Aktual dan Prediksi")
    st.dataframe(prices_df)

    # Grafik Harga Aktual vs Prediksi
    st.subheader("Grafik Harga Aktual vs Prediksi")
    st.line_chart(prices_df)

    # Menggabungkan dengan nama tipe bangunan
    # Membuat DataFrame perbandingan harga prediksi dengan tipe bangunan
    type_data = type_data.merge(prices_df, left_index=True, right_index=True)
    type_data['Type'] = label_encoder.inverse_transform(type_data['Type_indexed'])

    # Grafik perbandingan harga prediksi per tipe bangunan
    st.subheader("Grafik Perbandingan Harga Prediksi per Tipe Bangunan")
    type_group = type_data.groupby('Type').agg({'Harga Prediksi': 'mean'}).reset_index()

    st.bar_chart(type_group.set_index('Type')['Harga Prediksi'])
else:
    st.write("Belum ada data baru untuk diproses.")
