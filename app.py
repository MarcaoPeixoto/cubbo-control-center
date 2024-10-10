from flask import Flask, render_template, send_from_directory, request, jsonify, session, redirect
from flask_cors import CORS
import json
import os
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
from functools import wraps
import redis
from dotenv import dotenv_values
import threading
from manifesto import save_to_google_docs, link_docs, nao_despachados, get_manifesto
from datetime import datetime, timedelta
from remocoes import get_remocoes  # Import the get_remocoes function
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing if needed
scheduler = BackgroundScheduler()
#t
# Set a secret key for sessions
app.secret_key = os.urandom(24)

# Get password from environment variable
CORRECT_PASSWORD = os.environ.get('LOGIN_PASSWORD')
REMOCOES_FOLDER_ID = os.environ.get('REMOCOES_FOLDER_ID')

env_config = dotenv_values(".env")

redis_end = env_config.get('REDIS_END')

if redis_end is not None:
    redis_port = env_config.get('REDIS_PORT')
    redis_password = env_config.get('REDIS_PASSWORD')
else:
    redis_end=os.environ["REDIS_END"]
    redis_port=os.environ["REDIS_PORT"]
    redis_password=os.environ["REDIS_PASSWORD"]

redis_client = redis.StrictRedis(host=redis_end, port=redis_port, password=redis_password, db=0, decode_responses=True)


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated_function

def job_embu():
    try:
        subprocess.run(['python', 'incentivosEmbu.py'])
        print("SLAs Embu atualizados")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred in Embu job: {e}")

def job_extrema():
    try:
        subprocess.run(['python', 'incentivosExtrema.py'])
        print("SLAs Extrema atualizados")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred in Extrema job: {e}")

def job_bonus():
    try:
        subprocess.run(['python', 'bonus.py'])
        print("SLAs Extrema atualizados")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred in Bonus job: {e}")

scheduler.add_job(job_embu, 'interval', minutes=5, max_instances=10000)
scheduler.add_job(job_extrema, 'interval', minutes=7, max_instances=10000)
scheduler.add_job(job_bonus, 'interval', minutes=3, max_instances=10000)

@app.before_request
def start_scheduler():
    if not scheduler.running:
        scheduler.start()

@app.route('/')
def inicio():
    return redirect('/login')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        data = request.json
        if data and data.get('password') == CORRECT_PASSWORD:
            session['logged_in'] = True
            return jsonify({"success": True})
        return jsonify({"success": False}), 401
    return render_template('index.html')

@app.route('/check_auth')
def check_auth():
    return jsonify({"authenticated": 'logged_in' in session})

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    return redirect('/login')

@app.route('/home')
@login_required
def home():
    return render_template('home.html')

@app.route('/manifesto', methods=['GET', 'POST'])
@login_required
def manifesto_route():
    if request.method == 'POST':
        transportadora = request.form.get('manifesto_option')
        print(f"Selected transportadora: {transportadora}")
        if transportadora:
            try:
                document_url = link_docs(transportadora)
                if document_url:
                    data = get_manifesto(transportadora)
                    not_dispatched_count = nao_despachados(data)
                    return render_template('manifesto.html', not_dispatched_count=not_dispatched_count, document_url=document_url)
                else:
                    return render_template('manifesto.html', error="Failed to create the document.")
            except Exception as e:
                return render_template('manifesto.html', error=f"An error occurred: {str(e)}")
        else:
            return render_template('manifesto.html', error="Please select a valid option.")
    return render_template('manifesto.html')


@app.route('/bonus')
@login_required
def bonus():
    return render_template('bonus.html')

@app.route('/ops')
@login_required
def ops():
    return render_template('ops.html')

@app.route('/embu')
@login_required
def embu():
    return render_template('dashCUBBOembu.html')

@app.route('/extrema')
@login_required
def extrema():
    return render_template('dashCUBBOextrema.html')

@app.route('/controlembu')
@login_required
def controle_sp():
    return render_template('controleEmbu.html')

@app.route('/controlextrema')
@login_required
def controle_mg():
    return render_template('controleExtrema.html')

@app.route('/remocoes')
@login_required
def remocoes():
    return render_template('remocoes.html')

@app.route('/json/<path:filename>')
@login_required
def serve_json(filename):
    return send_from_directory('json', filename)

from flask import jsonify, request
import json
import os
import threading

@app.route('/update-json', methods=['POST'])
@login_required
def update_json():
    new_data = request.get_json()

    if not new_data:
        return jsonify({"error": "No data provided"}), 400

    # Determine the JSON file path based on the 'local' field
    json_file_path = ''
    if new_data.get('local') == 'embu':
        json_file_path = 'json/sla_embu.json'
        redis_key = "sla_embu"
    elif new_data.get('local') == 'extrema':
        json_file_path = 'json/sla_extrema.json'
        redis_key = "sla_extrema"
    else:
        return jsonify({"error": "Invalid 'local' value"}), 400

    # Check if file exists and is accessible
    if not os.path.exists(json_file_path):
        return jsonify({"error": f"File {json_file_path} does not exist"}), 404

    try:
        # Acquire a lock to prevent concurrent access
        file_lock = threading.Lock()
        with file_lock, open(json_file_path, 'r') as file:
            json_data = json.load(file)

        # Safely update JSON data
        for key in ['ajuste_recibos', 'ajuste_picking', 'ajuste_pedidos']:
            if key in new_data:
                json_data[key] = new_data[key]

        # Write the updated data back to the file
        with file_lock, open(json_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)

        # Save the updated data to Redis
        try:
            save_to_redis(redis_key, json_data)
            print(f"Data saved to Redis for {redis_key}")
        except Exception as e:
            app.logger.error(f"Failed to save data to Redis: {e}")
            return jsonify({"error": f"Failed to save to Redis: {str(e)}"}), 500

        return jsonify(json_data)

    except (IOError, json.JSONDecodeError) as e:
        app.logger.error(f"Error processing JSON file: {e}")
        return jsonify({"error": f"Error processing JSON file: {str(e)}"}), 500


@app.route('/update-excluded-orders', methods=['POST'])
@login_required
def update_excluded_orders():
    try:
        new_data = request.get_json()
        json_file_path = 'json/excluded_orders.json'
        
        # Read existing data
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)
        
        # Ensure json_data is a dictionary with 'excluded_orders' key
        if not isinstance(json_data, dict):
            json_data = {'excluded_orders': []}
        elif 'excluded_orders' not in json_data:
            json_data['excluded_orders'] = []
        
        # Append new excluded order
        json_data['excluded_orders'].append(new_data['excluded_order'])
        
        # Write updated data back to file
        with open(json_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)
        
        # Save to Redis
        save_to_redis("excluded_orders", json_data)
        
        return jsonify(json_data)
    
    except Exception as e:
        app.logger.error('Error updating excluded orders JSON: %s', e)
        return jsonify(error=str(e)), 500

@app.route('/update-excluded-recibos', methods=['POST'])
@login_required
def update_excluded_recibos():
    try:
        new_data = request.get_json()
        json_file_path = 'json/excluded_recibos.json'

        # Read existing data
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        # Ensure json_data is a dictionary with 'excluded_recibos' key
        if not isinstance(json_data, dict):
            json_data = {'excluded_recibos': []}
        elif 'excluded_recibos' not in json_data:
            json_data['excluded_recibos'] = []

        # Append new excluded recibo
        json_data['excluded_recibos'].append(new_data['excluded_recibo'])

        # Write updated data back to file
        with open(json_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)

        # Save to Redis
        save_to_redis("excluded_recibos", json_data)

        return jsonify(json_data)

    except Exception as e:
        app.logger.error('Error updating excluded recibos JSON: %s', e)
        return jsonify(error=str(e)), 500
    
#redis funtions

def save_to_redis(key, data):
    try:
        # Ensure data is not None and can be converted to JSON
        if data is None:
            raise ValueError("Cannot save None data to Redis.")
        json_data = json.dumps(data)
        redis_client.set(key, json_data)
    except Exception as e:
        print(f"Error saving data to Redis: {e}")
  
def load_from_redis(key):
    try:
        json_data = redis_client.get(key)
        if json_data is None:
            raise ValueError(f"No data found in Redis for key: {key}")
        return json.loads(json_data)
    except Exception as e:
        print(f"Error loading data from Redis: {e}")
        return {}

# Adjust functions like load_excluded_orders and save_excluded_orders to use Redis
def load_excluded_orders():
    return load_from_redis("excluded_orders")

def load_excluded_recibos():
    return load_from_redis("excluded_recibos")

def save_excluded_recibos(excluded_recibos):
    with open("json/excluded_recibos.json", "w") as file:
        json.dump(excluded_recibos, file)

def save_excluded_orders(excluded_orders):
    with open("json/excluded_orders.json", "w") as file:
        json.dump(excluded_orders, file)

def load_sla_embu():
    return load_from_redis("sla_embu")

def load_sla_extrema():
    return load_from_redis("sla_extrema")

def save_sla_embu(sla_embu):
    with open("json/sla_embu.json", "w") as file:
        json.dump(sla_embu, file)

def save_sla_extrema(sla_extrema):
    with open("json/sla_extrema.json", "w") as file:
        json.dump(sla_extrema, file)
        

def update_jsons():
    excluded_recibos = load_excluded_recibos()
    excluded_orders = load_excluded_orders()
    sla_embu = load_sla_embu()
    sla_extrema = load_sla_extrema()
    save_excluded_recibos(excluded_recibos)
    save_excluded_orders(excluded_orders)
    save_sla_embu(sla_embu)
    save_sla_extrema(sla_extrema)
    print("jsons updated!")

def check_redis_connectivity():
    try:
        # Attempt to ping the Redis server
        response = redis_client.ping()
        if response:
            print("Connected to Redis successfully!")
            return True
    except redis.ConnectionError as e:
        print(f"Failed to connect to Redis: {e}")
        return False

@app.route('/api/remocoes')
@login_required
def api_remocoes():
    redis_key = "remocoes"
    remocoes_json = redis_client.get(redis_key)
    if remocoes_json:
        remocoes = json.loads(remocoes_json)
        return jsonify(remocoes)
    else:
        # If data is not in Redis, fetch it and store it
        remocoes = get_remocoes()
        return jsonify(remocoes)

@app.route('/upload-images', methods=['POST'])
def upload_images():

    
    SCOPES = ['https://www.googleapis.com/auth/documents.readonly', 
              'https://www.googleapis.com/auth/drive.file',
              'https://www.googleapis.com/auth/drive']
    
    if 'images' not in request.files:
        return jsonify({'success': False, 'error': 'No images in the request'}), 400

    id = request.form.get('id')
    if not id:
        return jsonify({'success': False, 'error': 'No ID provided'}), 400

    # Set up Google Drive API client
    creds = None
    token_json = redis_client.get('token_json')

    if token_json:
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing credentials: {e}")
                creds = None
        
        if not creds:
            credentials_json = redis_client.get('credentials_json')
            if not credentials_json:
                raise Exception("credentials.json not found in Redis")
            flow = InstalledAppFlow.from_client_config(
                json.loads(credentials_json), SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Update token in Redis
        redis_client.set('token_json', creds.to_json())
    drive_service = build('drive', 'v3', credentials=creds)

    folder_id = REMOCOES_FOLDER_ID

    uploaded_files = []
    for file in request.files.getlist('images'):
        filename = file.filename
        file_path = os.path.join('/tmp', filename)
        file.save(file_path)

        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, resumable=True)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        uploaded_files.append(file.get('id'))

        os.remove(file_path)  # Clean up the temporary file

    # Update the 'removido' status in your database for this ID
    update_removido_status(id)

    return jsonify({'success': True, 'uploaded_files': uploaded_files})

def update_removido_status(id):
    # Implement this function to update the 'removido' status in your database
    pass

if __name__ == '__main__':
    # Run both scripts initially
    check_redis_connectivity()
    update_jsons()
    scheduler.start()
    app.run(host='0.0.0.0', debug=True)
    