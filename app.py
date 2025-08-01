from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import pandas as pd
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Your database path
DB_PATH = "FPA_FOD_20170508.sqlite"

def ensure_sqlite_downloaded():
    """Download the database if it doesn't exist"""
    if not os.path.exists(DB_PATH):
        print("Downloading wildfire dataset from Hugging Face...")
        import requests
        url = "https://huggingface.co/datasets/aj0818/wildfire/resolve/main/FPA_FOD_20170508.sqlite"
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(DB_PATH, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download complete.")

@app.route('/api/wildfires')
def get_wildfires():
    """Get random sample of wildfire data"""
    # Get sample size from query parameter (default 3000)
    sample_size = request.args.get('limit', 3000, type=int)
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        df = pd.read_sql_query(
            "SELECT FIRE_YEAR, DISCOVERY_DOY, CONT_DOY, FIRE_SIZE, LATITUDE, LONGITUDE "
            "FROM Fires ORDER BY RANDOM() LIMIT ?", 
            conn, params=(sample_size,)
        )
        conn.close()
        
        # Clean the data like your original code
        df["DAY_TO_CONT"] = df["CONT_DOY"] - df["DISCOVERY_DOY"]
        df = df.dropna(subset=["LATITUDE", "LONGITUDE"]).reset_index(drop=True)
        
        # Convert to JSON-serializable format
        data = df.to_dict('records')
        
        return jsonify({
            'success': True,
            'count': len(data),
            'data': data
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/years')
def get_available_years():
    """Get all available years in the dataset"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT DISTINCT FIRE_YEAR FROM Fires ORDER BY FIRE_YEAR", conn)
        conn.close()
        
        years = df['FIRE_YEAR'].tolist()
        return jsonify({
            'success': True,
            'years': years
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/health')
def health_check():
    """Simple health check endpoint"""
    return jsonify({'status': 'healthy', 'database_exists': os.path.exists(DB_PATH)})

if __name__ == '__main__':
    # Ensure database exists
    ensure_sqlite_downloaded()
    
    print("Starting Flask server...")
    print("API endpoints:")
    print("  - GET /api/wildfires?limit=3000")
    print("  - GET /api/years")
    print("  - GET /api/health")
    
    # Run the server
    app.run(debug=True, host='0.0.0.0', port=5000)
