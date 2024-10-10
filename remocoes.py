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
from google.auth.exceptions import RefreshError
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


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
            'cliente': remocao['cliente'],
            'removido': False,  # Initialize as False
            'volumes': remocao.get('volumes', 1)  # Default to 1 if not present
        }

        processed_remocoes.append(data)

    # Sort processed_remocoes by numero_pedido
    processed_remocoes.sort(key=lambda x: (x['cliente'], x['numero_pedido']))

    # Store all removals under a single Redis key
    redis_key = "remocoes"
    redis_client.set(redis_key, json.dumps(processed_remocoes))

    return processed_remocoes

def check_removido_status(numero_pedido, cliente, max_retries=3, delay=1):
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
    
    for attempt in range(max_retries):
        try:
            creds = None
            token_json = redis_client.get('token_json')

            if token_json:
                creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    try:
                        creds.refresh(Request())
                    except RefreshError:
                        # If refresh fails, force re-authentication
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

            folder_id = os.environ.get('REMOCOES_FOLDER_ID')
            query = f"'{folder_id}' in parents and (name contains '{numero_pedido}' and name contains '{cliente}')"

            results = drive_service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
            files = results.get('files', [])

            return len(files) > 0

        except HttpError as e:
            if e.resp.status in [403, 429]:  # Rate limit error
                if attempt < max_retries - 1:
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
                    continue
            print(f"HTTP error occurred: {e}")
            return False
        except Exception as e:
            print(f"Error checking removido status: {e}")
            return False

    print(f"Max retries reached for checking removido status: {numero_pedido}, {cliente}")
    return False

get_remocoes()