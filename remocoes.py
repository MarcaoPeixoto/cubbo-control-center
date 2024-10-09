from datetime import datetime, timedelta
import os
import json
import requests
from dotenv import dotenv_values
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import redis


date_format = "%d-%m-%Y, %H:%M:%S"

date_format2 = "%d-%m-%Y, %H:%M:%S.%f"


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

def create_metabase_token():

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

def get_remocoes():
    remocoes = get_dataset('3509')

    processed_remocoes = []

    for remocao in remocoes:
        try:
            remocao['pendente'] = datetime.strptime(remocao['pendente'], date_format)
            remocao['processado'] = datetime.strptime(remocao['processado'], date_format)
        except:
            remocao['pendente'] = datetime.strptime(remocao['pendente'], date_format2)
            remocao['processado'] = datetime.strptime(remocao['processado'], date_format2)

        data = {
            'id': remocao['id'],
            'pendente': remocao['pendente'].strftime(date_format) if remocao['pendente'] else None,
            'processado': remocao['processado'].strftime(date_format) if remocao['processado'] else None,
            'numero_pedido': remocao['numero_pedido'],
            'cliente': remocao['cliente']
        }

        processed_remocoes.append(data)

    # Store all removals under a single Redis key
    redis_key = "remocoes"
    redis_client.set(redis_key, json.dumps(processed_remocoes))



    return remocoes

def rem_link():
    folder_id = env_config.get('REMOCOES_FOLDER_ID')

    print(f"Using folder ID: {folder_id}")

get_remocoes()