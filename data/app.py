from flask import Flask, jsonify, request
import boto3
import pandas as pd
import io
import joblib
from sklearn.metrics.pairwise import euclidean_distances

app = Flask(__name__)

# Konfigurasi MinIO
minio_client = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="dua",
    aws_secret_access_key="kelompok2"
)

bucket_name = "property-data"

# Muat model prediksi
MODEL_PATH = "random_forest_model.pkl"
model = joblib.load(MODEL_PATH)

# Fungsi utilitas untuk membersihkan data
def preprocess_data(batch_data):
    """
    Melakukan preprocessing pada dataset, termasuk memetakan kolom 'Type' ke 'Type_indexed'.
    """
    type_mapping = {"h": 0, "t": 1, "u": 2}  # h = house, t = townhouse, u = unit
    if 'Type' in batch_data.columns:
        batch_data['Type_indexed'] = batch_data['Type'].map(type_mapping)
    return batch_data

def clean_data(data):
    """
    Membersihkan dataset dari nilai NaN agar aman digunakan.
    """
    if 'Type_indexed' not in data.columns and 'Type' in data.columns:
        data = preprocess_data(data)

    data.fillna({
        "Rooms": 0,
        "Type_indexed": 0,  # Default untuk tipe properti
        "Distance": data["Distance"].mean() if 'Distance' in data.columns else 0,
        "BuildingArea": data["BuildingArea"].mean() if 'BuildingArea' in data.columns else 0
    }, inplace=True)
    return data

# Fungsi utilitas untuk mengambil batch dari MinIO
def get_batches_from_minio():
    """
    Mengambil daftar semua batch yang ada di MinIO.
    """
    response = minio_client.list_objects_v2(Bucket=bucket_name)
    return [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].startswith("batch_")]

def get_data_from_batch(batch_name):
    """
    Mengambil data dari batch tertentu di MinIO dan memprosesnya.
    """
    response = minio_client.get_object(Bucket=bucket_name, Key=batch_name)
    batch_data = pd.read_csv(io.BytesIO(response['Body'].read()))
    batch_data = preprocess_data(batch_data)
    return batch_data

# Fungsi untuk menemukan properti terdekat berdasarkan jarak Euclidean
def find_closest_properties(all_data, target, n_recommendations=5):
    """
    Mencari properti yang paling mirip berdasarkan jarak Euclidean.
    """
    try:
        # Bersihkan data sebelum perhitungan
        all_data = clean_data(all_data)

        # Pilih kolom relevan untuk pencocokan
        features = ['Rooms', 'Type_indexed', 'Distance', 'BuildingArea']
        all_data_features = all_data[features].copy()
        target_features = pd.DataFrame([target])

        # Hitung jarak Euclidean
        distances = euclidean_distances(all_data_features, target_features)
        all_data['distance'] = distances

        # Ambil n properti terdekat
        recommendations = all_data.nsmallest(n_recommendations, 'distance').drop(columns=['distance'])
        return recommendations.to_dict(orient='records')
    except Exception as e:
        raise RuntimeError(f"Gagal mencari rekomendasi properti: {e}")

@app.route('/api/houses', methods=['GET'])
def get_filtered_houses():
    """
    API untuk mengambil data properti dari semua batch berdasarkan input tertentu.
    Jika tidak ada data yang cocok, akan memberikan rekomendasi properti yang mirip.
    """
    try:
        # Ambil parameter input
        rooms = request.args.get('Rooms')
        type_input = request.args.get('Type')
        distance = request.args.get('Distance')
        building_area = request.args.get('BuildingArea')

        if not (rooms and type_input and distance and building_area):
            return jsonify({"error": "Semua parameter (Rooms, Type, Distance, BuildingArea) harus diisi"}), 400

        rooms = int(rooms)
        distance = float(distance)
        building_area = float(building_area)

        # Konversi tipe ke indeks numerik
        type_mapping = {"h": 0, "t": 1, "u": 2}
        type_indexed = type_mapping.get(type_input.lower())
        if type_indexed is None:
            return jsonify({"error": "Tipe properti tidak valid. Gunakan 'h', 't', atau 'u'"}), 400

        # Ambil semua batch yang ada di MinIO
        batch_names = get_batches_from_minio()
        all_data = pd.DataFrame()

        # Gabungkan semua batch menjadi satu DataFrame
        for batch_name in batch_names:
            batch_data = get_data_from_batch(batch_name)
            all_data = pd.concat([all_data, batch_data], ignore_index=True)

        # Filter data berdasarkan input pengguna
        filtered_data = all_data[
            (all_data['Rooms'] == rooms) &
            (all_data['Type_indexed'] == type_indexed) &
            (all_data['Distance'] == distance) &
            (all_data['BuildingArea'] == building_area)
        ]

        if not filtered_data.empty:
            return jsonify(filtered_data.to_dict(orient='records'))
        else:
            target = {"Rooms": rooms, "Type_indexed": type_indexed, "Distance": distance, "BuildingArea": building_area}
            recommendations = find_closest_properties(all_data, target)
            return jsonify({
                "message": "Tidak ada data yang cocok ditemukan. Berikut adalah properti yang direkomendasikan:",
                "recommendations": recommendations
            })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)
