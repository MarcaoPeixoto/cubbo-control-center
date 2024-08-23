from flask import Flask, render_template, send_from_directory, request, jsonify
from flask_cors import CORS
import json
import os

app = Flask(__name__)
CORS(app)  # Enable Cross-Origin Resource Sharing if needed

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/embu')
def embu():
    return render_template('dashCUBBOembu.html')

@app.route('/extrema')
def extrema():
    return render_template('dashCUBBOextrema.html')

@app.route('/controlembu')
def controle_sp():
    return render_template('controleEmbu.html')

@app.route('/controlextrema')
def controle_mg():
    return render_template('controleExtrema.html')

@app.route('/json/<path:filename>')
def serve_json(filename):
    return send_from_directory('json', filename)

@app.route('/update-json', methods=['POST'])
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
    app.run(host='0.0.0.0', debug=True)
