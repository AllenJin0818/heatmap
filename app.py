from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import pandas as pd
import os
import requests

app = Flask(__name__)
CORS(app)

DB_PATH = "FPA_FOD_20170508.sqlite"

def ensure_sqlite_downloaded():
    if not os.path.exists(DB_PATH):
        print("Downloading wildfire dataset...")
        url = "https://huggingface.co/datasets/aj0818/wildfire/resolve/main/FPA_FOD_20170508.sqlite"
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(DB_PATH, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download complete.")

@app.route('/api/wildfires')
def get_wildfires():
    sample_size = request.args.get('limit', 3000, type=int)
    
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(
            "SELECT FIRE_YEAR, DISCOVERY_DOY, CONT_DOY, FIRE_SIZE, LATITUDE, LONGITUDE "
            "FROM Fires ORDER BY RANDOM() LIMIT ?", 
            conn, params=(sample_size,)
        )
        conn.close()
        
        df["DAY_TO_CONT"] = df["CONT_DOY"] - df["DISCOVERY_DOY"]
        df = df.dropna(subset=["LATITUDE", "LONGITUDE"]).reset_index(drop=True)
        data = df.to_dict('records')
        
        return jsonify({
            'success': True,
            'count': len(data),
            'data': data
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/health')
def health_check():
    return jsonify({'status': 'healthy', 'database_exists': os.path.exists(DB_PATH)})

if __name__ == '__main__':
    ensure_sqlite_downloaded()
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)