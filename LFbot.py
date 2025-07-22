import requests
import json
from datetime import datetime
from google_chat_interface import send_message
from metabase import get_dataset
import os
from dotenv import load_dotenv
from redis_connection import get_redis_connection

date_format="%Y-%m-%dT%H:%M:%S"
date_format2="%Y-%m-%dT%H:%M:%S.%f"


def load_previous_data_redis(redis_client, redis_key):
    data = redis_client.get(redis_key)
    if not data:
        return []
    try:
        loaded = json.loads(data)
        # Convert executed_at back to datetime
        for item in loaded:
            try:
                item['executed_at'] = datetime.strptime(item['executed_at'], date_format)
            except ValueError:
                item['executed_at'] = datetime.strptime(item['executed_at'], date_format2)
        return loaded
    except (json.JSONDecodeError, TypeError):
        return []

def save_new_data_redis(redis_client, redis_key, data):
    serializable_data = []
    for item in data:
        item_copy = item.copy()
        item_copy['executed_at'] = item_copy['executed_at'].strftime(date_format)
        serializable_data.append(item_copy)
    redis_client.set(redis_key, json.dumps(serializable_data))

def compare_data(old_data, new_data):
    changes = []
    
    # Convert old_data into a set of tuples with standardized datetime strings
    old_movements = set()
    for item in old_data:
        try:
            # Convert string to datetime if it's a string, then back to string for consistent comparison
            if isinstance(item['executed_at'], str):
                executed_at = datetime.strptime(item['executed_at'], date_format)
            else:
                executed_at = item['executed_at']
            # Convert to string in a standard format for comparison
            executed_at_str = executed_at.strftime(date_format)
            
            movement = (executed_at_str, item['produto'], item['loja'], 
                       item['previous_stock_quantity'], item['new_stock_quantity'])
            old_movements.add(movement)
        except ValueError:
            # Try alternative date format if first one fails
            if isinstance(item['executed_at'], str):
                executed_at = datetime.strptime(item['executed_at'], date_format2)
            else:
                executed_at = item['executed_at']
            executed_at_str = executed_at.strftime(date_format)
            
            movement = (executed_at_str, item['produto'], item['loja'], 
                       item['previous_stock_quantity'], item['new_stock_quantity'])
            old_movements.add(movement)
    
    # Check for new movements using the same string format
    for item in new_data:
        executed_at_str = item['executed_at'].strftime(date_format)
        movement = (executed_at_str, item['produto'], item['loja'], 
                   item['previous_stock_quantity'], item['new_stock_quantity'])
        if movement not in old_movements:
            changes.append(item)
    
    return changes

def status_lf_redis(redis_client, redis_key):
    itens_list = get_dataset('3920')
    new_data = []

    for item in itens_list:
        if item['action'] != 'Transfer':
            continue
        try:
            executed_at = datetime.strptime(item['executed_at'], date_format)
        except:
            executed_at = datetime.strptime(item['executed_at'], date_format2)
        new_data.append({
            'executed_at': executed_at,  # Store as datetime object
            'previous_stock_quantity': item['previous_stock_quantity'],
            'new_stock_quantity': item['new_stock_quantity'],
            'produto': item['Products'],
            'loja': item['Stores__name']
        })

    old_data = load_previous_data_redis(redis_client, redis_key)
    changes = compare_data(old_data, new_data)

    if changes:
        save_new_data_redis(redis_client, redis_key, new_data)
    return changes

def mensagem_lf():
    redis_client = get_redis_connection()
    redis_key = 'lf_status'
    changes = status_lf_redis(redis_client, redis_key)
    message = []
    if changes:
        message.append("Novos movimentos de estoque detectados")
        for change in changes:
            movement = change['new_stock_quantity'] - change['previous_stock_quantity']
            if movement == 1:
                message.append(f"🥳 ENCONTRADO 🥳 - {change['loja'].upper()}: {movement} unidade do produto: {change['produto']}")
            if movement > 1:
                message.append(f"🥳 ENCONTRADO 🥳 - {change['loja'].upper()}: {movement} unidades do produto: {change['produto']}")
            if movement == -1:
                message.append(f"😢 PERDIDO 😢 - {change['loja'].upper()}: {abs(movement)} unidade do produto: {change['produto']}")
            if movement < -1:
                message.append(f"😢 PERDIDO 😢 - {change['loja'].upper()}: {abs(movement)} unidades do produto: {change['produto']}")
    print(message)
    return message

url = os.getenv('LF_BOT_URL')

if __name__ == "__main__":
    msg_lojas = mensagem_lf()
    if msg_lojas:  # Só envia se houver mensagem
        send_message(msg_lojas, "teste-bot-marco", webhook_url=url)