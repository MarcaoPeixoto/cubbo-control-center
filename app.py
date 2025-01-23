from flask import Flask, render_template, send_from_directory, request, jsonify, session, redirect, send_file
from flask_cors import CORS
import json
import os
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
from functools import wraps
from dotenv import load_dotenv, dotenv_values
import threading
from manifesto import link_docs, nao_despachados, get_manifesto
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
from atrasos import update_transportadora_data, get_atrasos, count_atrasos_by_date_and_transportadora, count_atrasos_by_uf_and_transportadora, count_atrasos_by_transportadora_with_percentage, generate_sheets
import logging
from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from io import BytesIO
import base64
import time


app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing if needed
scheduler = BackgroundScheduler()
#t
# Set a secret key for sessions
app.secret_key = os.urandom(24)

# Load environment variables
load_dotenv()

env_config = dotenv_values(".env")


# Get environment variables
CORRECT_PASSWORD = os.getenv('LOGIN_PASSWORD') or os.environ.get('LOGIN_PASSWORD')
REMOCOES_FOLDER_ID = os.getenv('REMOCOES_FOLDER_ID') or os.environ.get('REMOCOES_FOLDER_ID')
RH_FOLDER_ID = os.getenv('RH_FOLDER_ID') or os.environ.get('RH_FOLDER_ID')

# Replace the existing redis_client creation with:
redis_client = get_redis_connection()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add this near the top with other imports and constants
SCOPES = [
        'https://www.googleapis.com/auth/documents.readonly',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ]

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

@app.route('/ct')
@login_required
def ct():
    return render_template('ct.html')

@app.route('/cs')
@login_required
def cs():
    return render_template('cs.html')

@app.route('/rh')
@login_required
def rh():
    return render_template('rh.html')

@app.route('/advertencia', methods=['GET', 'POST'])
@login_required
def advertencia():
    if request.method == 'POST':
        output_path = None
        try:
            # Get form data
            nome_colaborador = request.form.get('nome_colaborador')
            cpf_colaborador = request.form.get('cpf_colaborador')
            cargo_colaborador = request.form.get('cargo_colaborador')
            nome_representante = request.form.get('nome_representante')
            cargo_representante = request.form.get('cargo_representante')
            assinatura_colaborador = request.form.get('assinaturaColaborador')
            assinatura_representante = request.form.get('assinaturaRepresentante')
            
            # Current date in São Paulo format
            current_date = datetime.now().strftime('São Paulo, %d/%m/%Y')

            # Create a temporary PDF with the signatures and text
            packet = BytesIO()
            c = canvas.Canvas(packet, pagesize=letter)
            
            # Adjust Y coordinates for tighter spacing
            c.setFont("Helvetica", 12)
            c.drawString(50, 280, current_date)  # Date
            c.drawString(50, 250, f"Nome do Colaborador: {nome_colaborador}")  # Moved up
            c.drawString(50, 235, f"CPF: {cpf_colaborador}")  # 15 pixels below nome
            c.drawString(50, 220, f"Cargo: {cargo_colaborador}")  # 15 pixels below CPF
            
            # Add colaborador signature
            if assinatura_colaborador:
                try:
                    img_data = base64.b64decode(assinatura_colaborador.split(',')[1])
                    temp_img_path = f"temp/sig_col_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
                    
                    with open(temp_img_path, 'wb') as img_file:
                        img_file.write(img_data)
                    
                    c.drawImage(temp_img_path, 50, 160, 200, 50)  # Signature space
                    os.remove(temp_img_path)
                    
                except Exception as e:
                    print(f"Error processing colaborador signature: {e}")
            
            # Representante info with tighter spacing
            c.drawString(50, 140, f"Nome do Representante da Empresa: {nome_representante}")
            c.drawString(50, 125, f"Cargo: {cargo_representante}")  # 15 pixels below nome
            
            # Add representante signature
            if assinatura_representante:
                try:
                    img_data = base64.b64decode(assinatura_representante.split(',')[1])
                    temp_img_path = f"temp/sig_rep_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
                    
                    with open(temp_img_path, 'wb') as img_file:
                        img_file.write(img_data)
                    
                    c.drawImage(temp_img_path, 50, 65, 200, 50)  # Signature space
                    os.remove(temp_img_path)
                    
                except Exception as e:
                    print(f"Error processing representante signature: {e}")
            
            c.save()
            packet.seek(0)
            
            # Create a new PDF with all pages
            new_pdf = PdfReader(packet)
            existing_pdf = PdfReader("static/advertencia.pdf")
            output = PdfWriter()

            # Extract filename without extension from existing PDF path
            base_filename = os.path.splitext(os.path.basename("static/advertencia.pdf"))[0]
            
            # Copy all pages except the last one as-is
            for i in range(len(existing_pdf.pages) - 1):
                output.add_page(existing_pdf.pages[i])
            
            # Merge the last page with our new content
            last_page = existing_pdf.pages[-1]
            last_page.merge_page(new_pdf.pages[0])
            output.add_page(last_page)
            
            # Generate unique filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            output_path = f"temp/{base_filename}_{timestamp}.pdf"
            
            # Write the output file
            with open(output_path, "wb") as output_file:
                output.write(output_file)
            
            # Upload to Google Drive
            creds = get_google_credentials()
            drive_service = build('drive', 'v3', credentials=creds)
            
            file_metadata = {
                'name': f'{base_filename}_{cpf_colaborador}_{datetime.now().strftime('%d_%m_%Y')}.pdf',
                'parents': [RH_FOLDER_ID]
            }
            
            # Create MediaFileUpload object and immediately use it
            with open(output_path, 'rb') as file:
                media = MediaFileUpload(output_path, mimetype='application/pdf', resumable=True)
                file = drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
            
            # Close and delete the file
            media._fd.close()
            time.sleep(0.5)
            
            if os.path.exists(output_path):
                os.remove(output_path)
            
            return jsonify({'success': True, 'file_id': file.get('id')})
            
        except Exception as e:
            print(f"Error in advertencia: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
            
        finally:
            try:
                if output_path and os.path.exists(output_path):
                    time.sleep(0.5)
                    os.remove(output_path)
            except Exception as e:
                print(f"Error cleaning up file: {str(e)}")
            
    return render_template('advertencia.html')

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

@app.route('/bonus_projetor')
@login_required
def bonus_projetor():
    return render_template('bonus_projetor.html')

@app.route('/ops')
@login_required
def ops():
    return render_template('ops.html')

@app.route('/atrasos')
@login_required
def atrasos():
    return render_template('atrasos.html')

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
    """
    Check and update the 'removido' status for all remocoes based on Redis data.
    """
    try:
        # Get current remocoes data
        remocoes_json = redis_client.get("remocoes")
        if not remocoes_json:
            print("No remocoes found in Redis")
            return False
        
        # Get current removed orders set
        removed_orders = redis_client.smembers("removed_orders")
        removed_orders = {str(order_id.decode()) if isinstance(order_id, bytes) else str(order_id) 
                         for order_id in removed_orders}
        
        # Parse and update remocoes
        remocoes = json.loads(remocoes_json)
        for remocao in remocoes:
            remocao['removido'] = str(remocao['id']) in removed_orders
        
        # Save updated data back to Redis
        redis_client.set("remocoes", json.dumps(remocoes))
        return True
        
    except Exception as e:
        print(f"Error in check_removido_status: {e}")
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
    try:
        # Update removido status
        check_removido_status()
        
        # Get updated remocoes data
        remocoes_json = redis_client.get("remocoes")
        search_query = request.args.get('search', '').lower()
        
        if remocoes_json:
            remocoes = json.loads(remocoes_json)
        else:
            remocoes = get_remocoes()
            if not remocoes:
                return jsonify([])
        
        # Apply filters
        if search_query:
            remocoes = [r for r in remocoes if
                       search_query in str(r['id']).lower() or
                       search_query in str(r.get('numero_pedido', '')).lower() or
                       search_query in str(r.get('cliente', '')).lower() or
                       search_query in str(r.get('pendente', '')).lower() or
                       search_query in str(r.get('processado', '')).lower()]
        else:
            # Only show non-removed items when no search is active
            remocoes = [r for r in remocoes if not r.get('removido', False)]
        
        return jsonify(remocoes)
        
    except Exception as e:
        print(f"Error in api_remocoes: {e}")
        return jsonify({'error': str(e)}), 500


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
    try:
        app.logger.info("Starting upload_images function")
        
        # Check if Redis is connected
        try:
            redis_client.ping()
        except Exception as e:
            app.logger.error(f"Redis connection error: {e}")
            return jsonify({'success': False, 'error': 'Database connection error'}), 500

        # Validate request
        if 'images' not in request.files:
            app.logger.error("No images found in request.files")
            return jsonify({'success': False, 'error': 'No images in the request'}), 400

        id = request.form.get('id')
        volumes = request.form.get('volumes')
        app.logger.info(f"Received request - ID: {id}, Volumes: {volumes}")
        
        if not id or not volumes:
            app.logger.error("Missing ID or volumes in request")
            return jsonify({'success': False, 'error': 'No ID or volumes provided'}), 400

        # Get remocao details from Redis
        redis_key = "remocoes"
        try:
            remocoes_json = redis_client.get(redis_key)
            if not remocoes_json:
                app.logger.error("No remocoes data found in Redis")
                return jsonify({'success': False, 'error': 'Remocoes not found in Redis'}), 404
            
            remocoes = json.loads(remocoes_json)
            remocao = next((r for r in remocoes if str(r['id']) == str(id)), None)
            
            if not remocao:
                app.logger.error(f"Remocao with ID {id} not found in data")
                return jsonify({'success': False, 'error': 'Remocao not found'}), 404
                
            app.logger.info(f"Found remocao: {remocao}")
            
        except redis.RedisError as e:
            app.logger.error(f"Redis error: {str(e)}")
            return jsonify({'success': False, 'error': f'Database error: {str(e)}'}), 500
        except json.JSONDecodeError as e:
            app.logger.error(f"JSON decode error: {str(e)}")
            return jsonify({'success': False, 'error': 'Invalid data format in database'}), 500

        # Check Google Drive configuration
        if not REMOCOES_FOLDER_ID:
            app.logger.error("REMOCOES_FOLDER_ID not configured")
            return jsonify({'success': False, 'error': 'Drive configuration missing'}), 500

        # Get Google Drive credentials
        try:
            token_json = redis_client.get('token_json')
            if not token_json:
                app.logger.error("No Google token found in Redis")
                return jsonify({'success': False, 'error': 'Google authentication not configured'}), 500

            creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
            
            if not creds.valid:
                if creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    app.logger.error("Invalid credentials and cannot refresh")
                    return jsonify({'success': False, 'error': 'Invalid Google credentials'}), 500

            redis_client.set('token_json', creds.to_json())
            
        except Exception as e:
            app.logger.error(f"Google credentials error: {str(e)}")
            return jsonify({'success': False, 'error': 'Google authentication error'}), 500

        # Process files
        try:
            drive_service = build('drive', 'v3', credentials=creds)
            uploaded_files = []
            
            for file in request.files.getlist('images'):
                if not file.filename:
                    continue
                    
                current_date = datetime.now().strftime('%d-%m-%Y')
                base_filename = f"{remocao['numero_pedido']}_{remocao['cliente']}_{volumes}_volumes_{current_date}"
                file_extension = os.path.splitext(file.filename)[1]
                filename = f"{base_filename}{file_extension}"
                
                # Use os.path.normpath to ensure correct path separators for the platform
                file_path = os.path.normpath(os.path.join(os.getcwd(), 'temp', filename))
                
                # Ensure the temp directory exists
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                try:
                    app.logger.info(f"Saving file to: {file_path}")
                    file.save(file_path)
                    
                    file_metadata = {
                        'name': filename,
                        'parents': [REMOCOES_FOLDER_ID]
                    }
                    media = MediaFileUpload(file_path, resumable=True)
                    file_result = drive_service.files().create(
                        body=file_metadata, 
                        media_body=media, 
                        fields='id'
                    ).execute()
                    
                    uploaded_files.append(file_result.get('id'))
                    app.logger.info(f"Successfully uploaded: {filename}")
                    
                finally:
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                            app.logger.info(f"Cleaned up temporary file: {file_path}")
                        except Exception as e:
                            app.logger.warning(f"Failed to remove temporary file {file_path}: {e}")

            if uploaded_files:
                redis_client.sadd("removed_orders", remocao['id'])
                check_removido_status()
                return jsonify({'success': True, 'uploaded_files': uploaded_files})
            else:
                return jsonify({'success': False, 'error': 'No files were uploaded'}), 400

        except Exception as e:
            app.logger.error(f"File processing error: {str(e)}")
            return jsonify({'success': False, 'error': f'Error processing files: {str(e)}'}), 500

    except Exception as e:
        app.logger.error(f"Unexpected error: {str(e)}")
        return jsonify({'success': False, 'error': f'An unexpected error occurred: {str(e)}'}), 500

def get_google_credentials():
    SCOPES = [
        'https://www.googleapis.com/auth/documents.readonly',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    
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

# Add a global variable to store the latest data
latest_atrasos_data = None

@app.route('/update_atrasos', methods=['POST'])
def update_atrasos_data():
    try:
        global latest_atrasos_data  # Declare we'll use the global variable
        
        # Get filter parameters from the request body
        data = request.json
        marca = data.get('marca')
        transportadora = data.get('transportadora')
        data_inicial = data.get('data_inicial') or datetime.now().strftime("%Y-%m-%d")
        data_final = data.get('data_final') or datetime.now().strftime("%Y-%m-%d")
        status = data.get('status')

        # Call update_redis_data with filter parameters
        updated_data = update_transportadora_data(
            transportadora=transportadora if transportadora else None,
            data_inicial=data_inicial,
            data_final=data_final,
            cliente=marca if marca else None,
            status=status if status else None
        )
        
        # Store the data globally
        latest_atrasos_data = updated_data

        return jsonify({"success": True, **updated_data})
    except Exception as e:
        app.logger.error(f"Error in update_atrasos_data: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/generate-sheets', methods=['POST'])
@login_required
def generate_sheets_route():
    global latest_atrasos_data
    
    try:
        if not latest_atrasos_data:
            return jsonify({
                'success': False,
                'error': 'No data available. Please update the data first.'
            }), 400

        spreadsheet_id = generate_sheets(data=latest_atrasos_data)
        return jsonify({
            'success': True,
            'spreadsheet_id': spreadsheet_id
        })
    except Exception as e:
        app.logger.error(f"Error generating sheets: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


if __name__ == '__main__':
    try:
        check_redis_connectivity()
        update_jsons()
        scheduler.start()
        app.run(host='0.0.0.0', debug=True)
    except Exception as e:
        logger.error(f"Failed to start the application: {e}")
