import requests
import json
from datetime import datetime
from google_chat_interface import send_message
from metabase import get_dataset
import os
from dotenv import load_dotenv
from parseDT import parse_date

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
        # Parse datetime strings using robust parsing
        pending_at = parse_date(pedido['pending_at'])
        picking_completed_at = parse_date(pedido['Picking Orders__completed_at'])
        
        # Skip if date parsing failed
        if pending_at is None or picking_completed_at is None:
            continue
        
        # Format the message
        formatted_message = f"Order: {pedido['order_number']}, Pendente: {pending_at.strftime('%d/%m/%y')}, Status: {pedido['Invoices__status']}, Picking Completo: {picking_completed_at.strftime('%d/%m/%y %H:%M')}"
        message.append(formatted_message)
    return message

if __name__ == "__main__":
    msg_pedidos_natura = nf_erro()
    send_message(msg_pedidos_natura, "nf-erro-natura", webhook_url=url)  # Send to a specific Google Chat space