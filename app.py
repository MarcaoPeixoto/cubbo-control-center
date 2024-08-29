from flask import Flask, render_template, send_from_directory, request, jsonify, session, redirect
from flask_cors import CORS
import json
import os
from apscheduler.schedulers.background import BackgroundScheduler
import subprocess
from functools import wraps

app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing if needed
scheduler = BackgroundScheduler()

# Set a secret key for sessions
app.secret_key = os.urandom(24)

# Get password from environment variable
CORRECT_PASSWORD = os.environ.get('LOGIN_PASSWORD')

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

scheduler.add_job(job_embu, 'interval', minutes=5)
scheduler.add_job(job_extrema, 'interval', minutes=7)

@app.before_request
def start_scheduler():
    if not scheduler.running:
        scheduler.start()

@app.route('/')
def inico():
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

@app.route('/json/<path:filename>')
@login_required
def serve_json(filename):
    return send_from_directory('json', filename)

@app.route('/update-json', methods=['POST'])
@login_required
def update_json():
    new_data = request.get_json()

    if new_data['local'] == 'embu':
        json_file_path = 'json/sla_embu.json'
    elif new_data['local'] == 'extrema':
        json_file_path = 'json/sla_extrema.json'

    try:
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        if 'ajuste_recibos' in new_data:
            json_data['ajuste_recibos'] = new_data['ajuste_recibos']
        if 'ajuste_picking' in new_data:
            json_data['ajuste_picking'] = new_data['ajuste_picking']
        if 'ajuste_pedidos' in new_data:
            json_data['ajuste_pedidos'] = new_data['ajuste_pedidos']

        with open(json_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)

        return jsonify(json_data)

    except Exception as e:
        app.logger.error('Error updating JSON: %s', e)
        return jsonify(error=str(e)), 500

@app.route('/update-excluded-orders', methods=['POST'])
@login_required
def update_excluded_orders():
    try:
        new_data = request.get_json()
        json_file_path = 'json/excluded_orders.json'

        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        if isinstance(json_data, list):
            json_data.append(new_data['excluded_order'])
        else:
            if 'excluded_orders' not in json_data:
                json_data['excluded_orders'] = []
            json_data['excluded_orders'].append(new_data['excluded_order'])

        with open(json_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)

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

        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        if isinstance(json_data, list):
            json_data.append(new_data['excluded_recibo'])
        else:
            if 'excluded_recibos' not in json_data:
                json_data['excluded_recibos'] = []
            json_data['excluded_recibos'].append(new_data['excluded_recibo'])

        with open(json_file_path, 'w') as file:
            json.dump(json_data, file, indent=2)

        return jsonify(json_data)
    except Exception as e:
        app.logger.error('Error updating excluded recibos JSON: %s', e)
        return jsonify(error=str(e)), 500

if __name__ == '__main__':
    # Run both scripts initially
    subprocess.run(['python', 'incentivosEmbu.py'])
    subprocess.run(['python', 'incentivosExtrema.py'])
    scheduler.start()
    app.run(host='0.0.0.0', debug=True)