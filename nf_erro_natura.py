import requests
import json
from datetime import datetime
from google_chat_interface import send_message
from metabase import get_dataset
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get webhook URL from environment variable
url = os.getenv('NF_ERRO_NATURA_URL')


def nf_erro():
    pedidos_natura = get_dataset('11926')
    if pedidos_natura is None:
        return ["Sem NF com erro"]
    
    
    message = []
    for pedido in pedidos_natura:
        # Parse datetime strings
        pending_at = datetime.fromisoformat(pedido['pending_at'].replace('Z', '+00:00'))
        picking_completed_at = datetime.fromisoformat(pedido['Picking Orders__completed_at'].replace('Z', '+00:00'))
        
        # Format the message
        formatted_message = f"Order: {pedido['order_number']}, Pendente: {pending_at.strftime('%d/%m/%y')}, Status: {pedido['Invoices__status']}, Picking Completo: {picking_completed_at.strftime('%d/%m/%y %H:%M')}"
        message.append(formatted_message)
    return message

if __name__ == "__main__":
    msg_pedidos_natura = nf_erro()
    send_message(msg_pedidos_natura, "nf-erro-natura", webhook_url=url)  # Send to a specific Google Chat space