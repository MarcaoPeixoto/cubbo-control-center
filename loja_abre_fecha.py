import os
from dotenv import load_dotenv, dotenv_values
import requests
import json
from datetime import datetime
from google_chat_interface import send_message
from metabase import get_dataset
from redis_connection import get_redis_connection

load_dotenv()

# Load from .env file first, then fallback to system environment
env_config = dotenv_values(".env")
url = env_config.get('STATUS_LOJAS_BR_URL') or os.getenv('STATUS_LOJAS_BR_URL')
REDIS_STORE_KEY = 'store_status'  # Redis key for storing status


def load_previous_data_redis():
    r = get_redis_connection()
    data = r.get(REDIS_STORE_KEY)
    if data:
        return json.loads(data)
    return []

def save_new_data_redis(data):
    r = get_redis_connection()
    r.set(REDIS_STORE_KEY, json.dumps(data))

def compare_data(old_data, new_data):
    changes = []

    old_data_dict = {item['loja']: item['status'] for item in old_data}
    new_data_dict = {item['loja']: item['status'] for item in new_data}

    for loja, new_status in new_data_dict.items():
        old_status = old_data_dict.get(loja)
        
        if old_status is None:  # New store detected
            changes.append({
                'loja': loja,
                'old_status': None,
                'new_status': new_status
            })
        elif old_status != new_status:  # Existing store with a status change
            changes.append({
                'loja': loja,
                'old_status': old_status,
                'new_status': new_status
            })

    return changes

def status_loja():
    stores_list = get_dataset('2954')
    new_data = [{'loja': loja['marca'], 'status': loja['account_type']} for loja in stores_list]
    old_data = load_previous_data_redis()
    changes = compare_data(old_data, new_data)
    if changes:
        save_new_data_redis(new_data)
    return changes

def mensagem_lojas():
    changes = status_loja()
    message = []
    if changes:
        message.append("Atenção:")
        for change in changes:
            if change['old_status'] is None:  # New store detected
                if change['new_status'] == "PROSPECT_ACCOUNT":
                    new_status = "Loja FECHADA"
                elif change['new_status'] == "CUSTOMER_ACCOUNT":
                    new_status = "Loja ABERTA"
                
                message.append(f"Nova Loja criada! Loja: {change['loja']}, {new_status}")
            else:
                if change['new_status'] == "PROSPECT_ACCOUNT":
                    new_status = "Loja FECHADA"
                elif change['new_status'] == "CUSTOMER_ACCOUNT":
                    new_status = "Loja ABERTA"

                message.append(f"Loja: {change['loja']}, {new_status}")
    
    return message



if __name__ == "__main__":
    msg_lojas = mensagem_lojas()
    if msg_lojas:
        send_message(msg_lojas, "status-lojas-br", webhook_url=url)