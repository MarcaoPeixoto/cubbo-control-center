import os
from dotenv import load_dotenv, dotenv_values
import requests
import json
from datetime import datetime
from google_chat_interface import send_message
from metabase import get_dataset

load_dotenv()

# Load from .env file first, then fallback to system environment
env_config = dotenv_values(".env")
url = env_config.get('STATUS_LOJAS_BR_URL') or os.getenv('STATUS_LOJAS_BR_URL')


def load_previous_data(filepath):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def save_new_data(filepath, data):
    # Ensure the directory exists before writing the file
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

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

def status_loja(filepath):
    stores_list = get_dataset('2954')
    
    new_data = [{'loja': loja['marca'], 'status': loja['account_type']} for loja in stores_list]

    old_data = load_previous_data(filepath)

    changes = compare_data(old_data, new_data)

    if changes:
        save_new_data(filepath, new_data)
    
    return changes

def mensagem_lojas():
    filepath = os.getenv('STORE_STATUS_PATH')
    changes = status_loja(filepath)
    message = []
    if changes:
        message.append("Atenção <!channel> : ")
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