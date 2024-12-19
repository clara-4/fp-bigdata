from flask import Flask, jsonify, send_from_directory, request
import pandas as pd

app = Flask(__name__, static_folder='styles', static_url_path='/styles')

# Load the CSV file
csv_file_path = 'kmeans_with_cluster_output.csv'
data = pd.read_csv(csv_file_path)

@app.route('/api/houses', methods=['GET'])
def get_houses():
    """API endpoint to get data from the CSV file with optional filters."""
    # Get query parameters
    rooms = request.args.get('rooms')
    bedroom2 = request.args.get('bedroom2')
    luasgedung = request.args.get('luasgedung')
    jarak = request.args.get('jarak')
    harga = request.args.get('harga')
    
    # Create a copy of the dataframe to filter
    filtered_data = data.copy()

    # Apply filters if parameters are provided
    if rooms and rooms.strip():
        filtered_data = filtered_data[filtered_data['Rooms'] == int(rooms)]
    
    if bedroom2 and bedroom2.strip():
        filtered_data = filtered_data[filtered_data['Bedroom2'] == int(bedroom2)]
    
    if luasgedung and luasgedung.strip():
        # Exact match for building area
        filtered_data = filtered_data[filtered_data['BuildingArea'] == float(luasgedung)]
    
    if jarak and jarak.strip():
        # Exact match for distance
        filtered_data = filtered_data[filtered_data['Distance'] == float(jarak)]
    
    if harga and harga.strip():
        # Exact match for price
        filtered_data = filtered_data[filtered_data['Price'] == float(harga)]

    # If no filters are applied, return the entire dataset
    if filtered_data.empty:
        return jsonify(data.to_dict(orient='records'))
    
    return jsonify(filtered_data.to_dict(orient='records'))

@app.route('/')
def serve_html():
    """Serve the HTML frontend."""
    return send_from_directory('.', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    """Serve other static files (e.g., images, CSS)."""
    return send_from_directory('.', path)

if __name__ == '__main__':
    app.run(debug=True)
