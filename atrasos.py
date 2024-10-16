from datetime import datetime, timedelta, date
import os
import json
from dotenv import dotenv_values
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from redis_connection import get_redis_connection
import requests
from parseUF import parse_UF
from collections import defaultdict
from datetime import date
from collections import Counter
import re


date_format = "%d-%m-%Y"
date_format2 = "%d-%m-%Y, %H:%M"

# Replace the existing redis_client creation with:
redis_client = get_redis_connection()

# Replace the existing authentication code with this:
def authenticate_google_docs():
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

    return build('docs', 'v1', credentials=creds)

# Replace the existing docs_service initialization with this:
docs_service = authenticate_google_docs()

# Existing functions
def create_metabase_token():

    env_config = dotenv_values(".env")
    metabase_user = env_config.get('METABASE_USER')
    
    if metabase_user is not None:
        metabase_password = env_config.get('METABASE_PASSWORD')
    else:
        metabase_user = os.environ["METABASE_USER"]
        metabase_password = os.environ["METABASE_PASSWORD"]

    url = 'https://cubbo.metabaseapp.com/api/session'
    data = {
        'username': metabase_user,
        'password': metabase_password
    }

    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        return response.json().get('id')
    else:
        raise Exception(f'Failed to create token: {response.content}')
    
def get_dataset(question, params={}):
    METABASE_ENDPOINT = "https://cubbo.metabaseapp.com"
    METABASE_TOKEN = create_metabase_token()

    res = requests.post(METABASE_ENDPOINT + '/api/card/'+question+'/query/json',
                        headers={"Content-Type": "application/json",
                                 'X-Metabase-Session': METABASE_TOKEN},
                        params=params,
                        )
    print(res)
    dataset = res.json()
    #print(dataset)

    return dataset

def process_data(inputs):

    def create_param(tag, param_value):
        param = {}
        if type(param_value) == int:
            param['type'] = "number/="
            param['value'] = param_value
        elif type(param_value) == datetime:
            param['type'] = "date/single"
            param['value'] = f"{param_value:%Y-%m-%d}"
        else:
            param['type'] = "category"
            param['value'] = param_value

        param['target'] = ["variable", ["template-tag", tag]]
        return param

    params = []
    for input_name, input_value in inputs.items():
        if input_value is not None:
            param = create_param(input_name, input_value)
            params.append(param)

    return {'parameters': json.dumps(params)}

hoje = datetime.now().strftime("%Y-%m-%d")


data_inicial = datetime.strptime("2024-09-01", "%Y-%m-%d")
data_final = datetime.strptime("2024-09-30", "%Y-%m-%d")

def get_atrasos(transportadora=None, data_inicial=None, data_final=None, cliente=None, status=None):

    if data_inicial is None:
        data_inicial = hoje
    if data_final is None:
        data_final = hoje

    print(f"Getting atrasos for transportadora: {transportadora}, data_inicial: {data_inicial}, data_final: {data_final}, cliente: {cliente}, status: {status}")
    params = process_data({
        'transportadora': transportadora,
        'data_inicial': data_inicial,
        'data_final': data_final,
        'cliente': cliente,
        'shipping_status': status
    })

    dataset = get_dataset('3477', params)
    
    print(f"Retrieved {len(dataset)} orders from the dataset")

    atrasos = []

    #print(dataset)
    for order in dataset:
        # Check if order is a dictionary
        order['shipping_zip_code'] = parse_UF(order['shipping_zip_code'])

        order['UF'] = order['shipping_zip_code']

        # Ensure delivered_at is always a datetime object
        if order['delivered_at'] is not None and order['delivered_at'] != "":     
            try:
                order['delivered_at'] = datetime.strptime(order['delivered_at'], date_format)
            except ValueError:
                try:
                    order['delivered_at'] = datetime.strptime(order['delivered_at'], date_format2)
                except ValueError:
                    print(f"Warning: Unable to parse delivered_at date for order {order.get('order_number', 'Unknown')}")
                    order['delivered_at'] = datetime.now()  # Fallback to current date
        else:
            order['delivered_at'] = datetime.now()
            order['SLA'] = "MISS"

        # Ensure estimated_time_arrival is always a datetime object
        if order['estimated_time_arrival'] is not None and order['estimated_time_arrival'] != "":         
            try:
                order['estimated_time_arrival'] = datetime.strptime(order['estimated_time_arrival'], date_format)
            except ValueError:
                try:
                    order['estimated_time_arrival'] = datetime.strptime(order['estimated_time_arrival'], date_format2)
                except ValueError:
                    print(f"Warning: Unable to parse estimated_time_arrival date for order {order.get('order_number', 'Unknown')}")
                    order['estimated_time_arrival'] = order['delivered_at']  # Fallback to delivered_at
        else:
            order['estimated_time_arrival'] = order['delivered_at']

        # Now we can safely compare datetime objects
        if order['estimated_time_arrival'] < order['delivered_at']:
            atraso = order['delivered_at'] - order['estimated_time_arrival']
            order['SLA'] = "MISS"
        else:
            order['SLA'] = "HIT"
            atraso = timedelta(0)

        if order['processado'] is not None and order['processado'] != "":       
            try:
                order['processado'] = datetime.strptime(order['processado'], date_format)
            except ValueError:
                order['processado'] = datetime.strptime(order['processado'], date_format2)

        if order['first_delivery_attempt_at'] is not None and order['first_delivery_attempt_at'] != "":
            try:
                order['first_delivery_attempt_at'] = datetime.strptime(order['first_delivery_attempt_at'], date_format)
            except ValueError:
                order['first_delivery_attempt_at'] = datetime.strptime(order['first_delivery_attempt_at'], date_format2)   
                
            if order['first_delivery_attempt_at'].day < order['delivered_at'].day and order['first_delivery_attempt_at'].month == order['delivered_at'].month:
                order['first_delivery'] = "MISS"
            else:
                order['first_delivery'] = "HIT"
        else:
            order['first_delivery_attempt_at'] = order['delivered_at']
            order['first_delivery'] = "HIT"

        if order['SLA'] == "MISS":
            atrasos.append({
                'store_name': order.get('store_name', ''),
                'order_number': order.get('order_number', ''),
                'rastreio': order.get('rastreio', ''),
                'transportadora': order.get('carrier_name', ''),
                'UF': order['shipping_zip_code'],
                'processado': order['processado'],
                'first_delivery_attempt_at': order['first_delivery_attempt_at'],
                'shipping_status': order.get('shipping_status', ''),
                'delivered_at': order['delivered_at'],
                'estimated_time_arrival': order['estimated_time_arrival'],
                'SLA': order['SLA'],
                'first_delivery': order['first_delivery'],
                'atraso': atraso
            })

    print(f"Processed {len(atrasos)} atrasos")

    return atrasos

def count_atrasos_by_date_and_transportadora(atrasos):
    
    order_counts = defaultdict(lambda: defaultdict(int))

    for atraso in atrasos:
        transportadora = atraso['transportadora']
        processado_date = atraso['processado'].date()
        order_counts[processado_date][transportadora] += 1

    # Convert defaultdict to regular dict and sort dates
    return {
        date_key: dict(carriers)
        for date_key, carriers in sorted(order_counts.items())
    }

def count_atrasos_by_uf_and_transportadora(atrasos):
    order_counts = defaultdict(lambda: defaultdict(int))

    for atraso in atrasos:
        uf = atraso['UF']
        transportadora = atraso['transportadora']
        order_counts[uf][transportadora] += 1

    # Convert defaultdict to regular dict and sort UFs alphabetically
    return {
        uf: dict(carriers)
        for uf, carriers in sorted(order_counts.items())
    }

def count_atrasos_by_transportadora_with_percentage(atrasos):
    # Count orders by transportadora
    transportadora_counts = Counter(atraso['transportadora'] for atraso in atrasos)
    
    # Calculate total number of orders
    total_orders = sum(transportadora_counts.values())
    
    # Calculate percentages and create the result dictionary
    result = {}
    for transportadora, count in transportadora_counts.items():
        percentage = (count / total_orders) * 100
        result[transportadora] = {
            'count': count,
            'percentage': round(percentage, 2)  # Round to 2 decimal places
        }
    
    # Sort the result by count in descending order
    sorted_result = dict(sorted(result.items(), key=lambda x: x[1]['count'], reverse=True))
    
    return sorted_result

# Example usage:
# atrasos = get_atrasos(...)
# order_counts = count_atrasos_by_date_and_transportadora(atrasos)
# uf_order_counts = count_atrasos_by_uf_and_transportadora(atrasos)
# transportadora_stats = count_atrasos_by_transportadora_with_percentage(atrasos)

atrasos = get_atrasos(data_inicial=data_inicial, data_final=data_final)
order_counts = count_atrasos_by_date_and_transportadora(atrasos)
uf_order_counts = count_atrasos_by_uf_and_transportadora(atrasos)
transportadora_stats = count_atrasos_by_transportadora_with_percentage(atrasos)

# Modify the end of the file to save data to Redis
def update_redis_data():
    atrasos = get_atrasos(data_inicial=data_inicial, data_final=data_final)
    order_counts = count_atrasos_by_date_and_transportadora(atrasos)
    uf_order_counts = count_atrasos_by_uf_and_transportadora(atrasos)
    transportadora_stats = count_atrasos_by_transportadora_with_percentage(atrasos)

    # Convert date objects to strings in order_counts
    order_counts_serializable = {str(date): counts for date, counts in order_counts.items()}

    # Save data to Redis
    redis_client.set('order_counts', json.dumps(order_counts_serializable))
    redis_client.set('uf_order_counts', json.dumps(uf_order_counts))
    redis_client.set('transportadora_stats', json.dumps(transportadora_stats))
    redis_client.set('total_atrasos', len(atrasos))

# Run this function to update Redis data
update_redis_data()

# Optionally, you can keep the print statements for debugging
print(order_counts)
print(uf_order_counts)
print(transportadora_stats)
