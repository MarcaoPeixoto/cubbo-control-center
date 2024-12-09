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
from google_auth import authenticate_google
from dotenv import dotenv_values

env_config = dotenv_values(".env")

redis_client = get_redis_connection()

drive_service = authenticate_google('drive')


def get_remocoes():
    """Get and process removal orders from the dataset"""
    remocoes = get_dataset('3509')
    processed_remocoes = []
    
    # Get the set of removed order IDs from Redis
    removed_orders = redis_client.smembers("removed_orders")
    if not removed_orders:
        removed_orders = set()

    for remocao in remocoes:
        try:
            remocao['pendente'] = parse_date(remocao['pendente'])
            remocao['processado'] = parse_date(remocao['processado'])
        except ValueError:
            print(f"Error parsing date: {remocao['pendente']} or {remocao['processado']}")
            continue

        # Check if the order is in the removed_orders set
        is_removed = str(remocao['id']) in removed_orders

        data = {
            'id': remocao['id'],
            'pendente': remocao['pendente'].strftime('%d-%m-%Y') if remocao['pendente'] else None,
            'processado': remocao['processado'].strftime('%d-%m-%Y') if remocao['processado'] else None,
            'numero_pedido': remocao['numero_pedido'],
            'cliente': remocao['cliente'],
            'removido': is_removed,
            'volumes': remocao.get('volumes', 1)
        }

        processed_remocoes.append(data)

    processed_remocoes.sort(key=lambda x: (x['cliente'], x['numero_pedido']))
    redis_client.set("remocoes", json.dumps(processed_remocoes))

    return processed_remocoes
 
def check_removido_status(numero_pedido, cliente, max_retries=3, delay=1):
    """
    Check if a removal order has been processed by looking for matching files in Google Drive.
    Returns True if matching files are found, False otherwise.
    """
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
    
    for attempt in range(max_retries):
        try:
            drive_service = authenticate_google('drive')  # Use the existing authenticated service

            folder_id = os.environ.get('REMOCOES_FOLDER_ID') or os.getenv('REMOCOES_FOLDER_ID')
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