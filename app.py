from flask import Flask, render_template, send_from_directory, request, jsonify, session, redirect, send_file
from flask_cors import CORS
import json
import os
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
from functools import wraps
from dotenv import load_dotenv, dotenv_values
import threading
from manifesto import save_to_google_docs, link_docs, nao_despachados, get_manifesto
from datetime import datetime, timedelta
from remocoes import get_remocoes, check_removido_status
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from redis_connection import get_redis_connection
import redis

app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing if needed
scheduler = BackgroundScheduler()
#t
# Set a secret key for sessions
app.secret_key = os.urandom(24)

# Load environment variables
load_dotenv()

# Get environment variables
CORRECT_PASSWORD = os.getenv('LOGIN_PASSWORD')
REMOCOES_FOLDER_ID = os.getenv('REMOCOES_FOLDER_ID')

# Replace the existing redis_client creation with:
redis_client = get_redis_connection()

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
        action = request.form.get('action')
        print(f"Selected transportadora: {transportadora}")
        if transportadora:
            try:
                data = get_manifesto(transportadora)
                not_dispatched_count = nao_despachados(data, transportadora)
                
                if action == 'consulta':
                    # Only return the not_dispatched_count without creating a Google Doc
                    return render_template('manifesto.html', not_dispatched_count=not_dispatched_count)
                elif action == 'generate':
                    # Generate Google Doc as before
                    document_url = link_docs(transportadora)
                    if document_url:
                        return render_template('manifesto.html', not_dispatched_count=not_dispatched_count, document_url=document_url)
                    else:
                        return render_template('manifesto.html', error="Failed to create the document.")
                else:
                    return render_template('manifesto.html', error="Invalid action.")
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

# Remove the @app.route decorator
def check_removido_status():
    redis_key = "remocoes"
    remocoes_json = redis_client.get(redis_key)
    if remocoes_json:
        remocoes = json.loads(remocoes_json)
        
        # Get the set of removed order IDs
        removed_orders = redis_client.smembers("removed_orders")
        
        # Update the 'removido' status based on the removed_orders set
        for remocao in remocoes:
            remocao['removido'] = remocao['id'] in removed_orders

        redis_client.set(redis_key, json.dumps(remocoes))
        return True
    else:
        print("Remocoes not found in Redis")
        return False

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
    
    # Call check_removido_status to ensure up-to-date information
    check_removido_status()
    
    remocoes_json = redis_client.get(redis_key)
    search_query = request.args.get('search', '').lower()
    
    if remocoes_json:
        remocoes = json.loads(remocoes_json)
    else:
        # If data is not in Redis, fetch it and store it
        remocoes = get_remocoes()
    
    # Filter remocoes based on search query 
    if search_query:
        remocoes = [r for r in remocoes if
                    search_query in str(r['id']).lower() or
                    search_query in r['numero_pedido'].lower() or
                    search_query in r['cliente'].lower() or
                    (r['pendente'] and search_query in r['pendente'].lower()) or
                    (r['processado'] and search_query in r['processado'].lower())]
    else:
        # If no search query, filter out removido=True items
        remocoes = [r for r in remocoes if not r.get('removido', False)]
    
    return jsonify(remocoes)


@app.route('/update-volumes', methods=['POST'])
@login_required
def update_volumes():
    data = request.json
    id = data.get('id')
    volumes = data.get('volumes')

    if not id or not volumes:
        return jsonify({'error': 'Missing id or volumes'}), 400

    # Update the volumes in Redis
    redis_key = "remocoes"
    remocoes_json = redis_client.get(redis_key)
    if remocoes_json:
        remocoes = json.loads(remocoes_json)
        for remocao in remocoes:
            if remocao['id'] == id:
                remocao['volumes'] = volumes
                break
        redis_client.set(redis_key, json.dumps(remocoes))
        return jsonify({'success': True})
    else:
        return jsonify({'error': 'Remocoes not found in Redis'}), 404

@app.route('/upload-images', methods=['POST'])
@login_required
def upload_images():
    SCOPES = ['https://www.googleapis.com/auth/documents.readonly', 
              'https://www.googleapis.com/auth/drive.file',
              'https://www.googleapis.com/auth/drive']

    if 'images' not in request.files:
        return jsonify({'success': False, 'error': 'No images in the request'}), 400

    id = request.form.get('id')
    volumes = request.form.get('volumes')
    if not id or not volumes:
        return jsonify({'success': False, 'error': 'No ID or volumes provided'}), 400

    # Get remocao details from Redis
    redis_key = "remocoes"
    remocoes_json = redis_client.get(redis_key)
    if remocoes_json:
        remocoes = json.loads(remocoes_json)
        remocao = next((r for r in remocoes if r['id'] == id), None)
        if not remocao:
            return jsonify({'success': False, 'error': 'Remocao not found'}), 404
    else:
        return jsonify({'success': False, 'error': 'Remocoes not found in Redis'}), 404

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
    for index, file in enumerate(request.files.getlist('images')):
        # Generate a unique filename for each image
        current_date = datetime.now().strftime('%d-%m-%Y')
        base_filename = f"{remocao['numero_pedido']}_{remocao['cliente']}_{volumes}_volumes_{current_date}"
        file_extension = os.path.splitext(file.filename)[1]
        filename = f"{base_filename}{file_extension}"

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

    if uploaded_files:
        # Save the order ID to a new Redis set
        redis_client.sadd("removed_orders", remocao['id'])
        
        # Call check_removido_status to update the status
        check_removido_status()

    return jsonify({'success': True, 'uploaded_files': uploaded_files})

def get_google_credentials():
    SCOPES = ['https://www.googleapis.com/auth/documents.readonly', 
              'https://www.googleapis.com/auth/drive.file',
              'https://www.googleapis.com/auth/drive']
    
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
    
    return creds

def generate_filename(remocao, volumes, index, file):
    base_filename = f"{remocao['numero_pedido']}_{remocao['cliente']}_{volumes}_volumes_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    file_extension = os.path.splitext(file.filename)[1]
    return f"{base_filename}_{index + 1}{file_extension}"

def save_temp_file(file, filename):
    file_path = os.path.join('/tmp', filename)
    file.save(file_path)
    return file_path

def upload_to_drive(drive_service, filename, file_path, folder_id):
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, resumable=True)
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return file.get('id')

@app.route('/get-image/<numero_pedido>/<cliente>')
@login_required
def get_image(numero_pedido, cliente):
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
    
    creds = None
    token_json = redis_client.get('token_json')

    if token_json:
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            return jsonify({'error': 'Invalid credentials'}), 401

        # Update token in Redis
        redis_client.set('token_json', creds.to_json())

    drive_service = build('drive', 'v3', credentials=creds)

    folder_id = REMOCOES_FOLDER_ID
    query = f"'{folder_id}' in parents and (name contains '{numero_pedido}' and name contains '{cliente}')"

    try:
        results = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])

        if files:
            file = files[0]  # Get the first file
            file_id = file['id']
            request = drive_service.files().get_media(fileId=file_id)
            file_content = io.BytesIO(request.execute())
            
            # Create a data URL for the image
            file_content.seek(0)
            import base64
            image_base64 = base64.b64encode(file_content.read()).decode('utf-8')
            image_url = f"data:image/jpeg;base64,{image_base64}"
            
            return jsonify({'image_url': image_url})
        else:
            return jsonify({'error': 'No image found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/refresh-remocoes', methods=['POST'])
@login_required
def refresh_remocoes():
    try:
        # Call the get_remocoes function to fetch fresh data
        new_remocoes = get_remocoes()
        
        # Update the Redis cache with the new data
        redis_client.set("remocoes", json.dumps(new_remocoes))
        
        # Call check_removido_status to update the status
        check_removido_status()
        
        return jsonify({"success": True})
    except Exception as e:
        app.logger.error(f"Error refreshing remocoes: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    # Run both scripts initially
    check_redis_connectivity()
    update_jsons()
    scheduler.start()
    app.run(host='0.0.0.0', debug=True)