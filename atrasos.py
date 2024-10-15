from datetime import datetime, timedelta
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


date_format = "%d-%m-%Y, %H:%M:%S"
date_format2 = "%d-%m-%Y, %H:%M:%S.%f"

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

    return dataset

def process_data(inputs):

    def create_param(tag, param_value):
        param = {}
        if type(param_value) == int:
            param['type'] = "number/="
            param['value'] = param_value
        elif isinstance(param_value, datetime):
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

def get_atrasos(transportadora = None, data_inicial = None, data_final = None, cliente = None, status = None):

    params = process_data({
        'transportadora': transportadora,
        'data_inicial': data_inicial,
        'data_final': data_final,
        'cliente': cliente,
        'shipping_status': status
    })

    dataset = get_dataset(3477, params)

    atrasos = []

    for order in dataset:
        order['shipping_zip_code'] = parse_UF(order['UF'])
        try:
            order['first_delivery_attempt_at'] = datetime.strptime(order['first_delivery_attempt_at'], date_format2)
        except ValueError:
            order['first_delivery_attempt_at'] = datetime.strptime(order['first_delivery_attempt_at'], date_format)        
        try:
            order['delivered_at'] = datetime.strptime(order['delivered_at'], date_format2)
        except ValueError:
            order['delivered_at'] = datetime.strptime(order['delivered_at'], date_format)
        
        try:
            order['estimated_time_arrival'] = datetime.strptime(order['estimated_time_arrival'], date_format2)
        except ValueError:
            order['estimated_time_arrival'] = datetime.strptime(order['estimated_time_arrival'], date_format)
        
        try:
            order['processado'] = datetime.strptime(order['processado'], date_format2)
        except ValueError:
            order['processado'] = datetime.strptime(order['processado'], date_format)

        if order['estimated_time_arrival'].day > order['delivered_at'].day and order['estimated_time_arrival'].month == order['delivered_at'].month:
            atraso = order['delivered_at'] - order['estimated_time_arrival']
            order['SLA'] = "MISS"
        else:
            order['SLA'] = "HIT"
            atraso = 0
        if order['first_delivery_attempt_at'] is not None or order['first_delivery_attempt_at'] != "":
            if order['first_delivery_attempt_at'].day < order['delivered_at'].day and order['first_delivery_attempt_at'].month == order['delivered_at'].month:
                order['first_delivery'] = "MISS"
            else:
                order['first_delivery'] = "HIT"
        else:
            order['first_delivery_attempt_at'] = order['delivered_at']
            order['first_delivery'] = "HIT"


        atrasos.append({
            'store_name': order['store_name'],
            'order_number': order['order_number'],
            'rastreio': order['rastreio'],
            'transportadora': order['carrier_name'],
            'UF': order['UF'],
            'processado': order['processado'],
            'first_delivery_attempt_at': order['first_delivery_attempt_at'],
            'shipping_status': order['shipping_status'],
            'delivered_at': order['delivered_at'],
            'estimated_time_arrival': order['estimated_time_arrival'],
            'SLA': order['SLA'],
            'first_delivery': order['first_delivery'],
            'atraso': atraso
        })

    return atrasos



