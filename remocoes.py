import os
import json
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from redis_connection import get_redis_connection
from google.auth.exceptions import RefreshError
import time
from metabase import get_dataset
from parseDT import parse_date

redis_client = get_redis_connection()

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
            flow = InstalledAppFlow.from_client_config(json.loads(credentials_json), SCOPES)
            creds = flow.run_local_server(port=0)
        
        redis_client.set('token_json', creds.to_json())

    return build('docs', 'v1', credentials=creds)

docs_service = authenticate_google_docs()


def get_remocoes():
    remocoes = get_dataset('3509')

    processed_remocoes = []
    removidos_antigos = redis_client.get('removidos_antigos')

    # Convert removidos_antigos from JSON string to a list of integers
    if removidos_antigos:
        removidos_antigos = json.loads(removidos_antigos)
    else:
        removidos_antigos = []

    for remocao in remocoes:
        if remocao['id'] in removidos_antigos:
            continue
        try:
            remocao['pendente'] = parse_date(remocao['pendente'])
            remocao['processado'] = parse_date(remocao['processado'])
        except ValueError:
            print(f"Error parsing date: {remocao['pendente']} or {remocao['processado']}")
            continue
        data = {
            'id': remocao['id'],
            'pendente': remocao['pendente'].strftime('%d-%m-%Y') if remocao['pendente'] else None,
            'processado': remocao['processado'].strftime('%d-%m-%Y') if remocao['processado'] else None,
            'numero_pedido': remocao['numero_pedido'],
            'cliente': remocao['cliente'],
            'removido': False,
            'volumes': remocao.get('volumes', 1)
        }

        processed_remocoes.append(data)

    processed_remocoes.sort(key=lambda x: (x['cliente'], x['numero_pedido']))
    redis_client.set("remocoes", json.dumps(processed_remocoes))

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
                        creds = None
                
                if not creds:
                    credentials_json = redis_client.get('credentials_json')
                    if not credentials_json:
                        raise Exception("credentials.json not found in Redis")
                    flow = InstalledAppFlow.from_client_config(json.loads(credentials_json), SCOPES)
                    creds = flow.run_local_server(port=0)
                
                redis_client.set('token_json', creds.to_json())

            drive_service = build('drive', 'v3', credentials=creds)

            folder_id = os.environ.get('REMOCOES_FOLDER_ID')
            query = f"'{folder_id}' in parents and (name contains '{numero_pedido}' and name contains '{cliente}')"

            results = drive_service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
            files = results.get('files', [])

            return len(files) > 0

        except Exception as e:
            print(f"Error checking removido status: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay * (2 ** attempt))
            else:
                return False

    print(f"Max retries reached for checking removido status: {numero_pedido}, {cliente}")
    return False

get_remocoes()