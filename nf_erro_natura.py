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
    pedidos_natura = get_dataset('8790')
    if pedidos_natura is None:
        return ["Sem NF com erro"]
    
    
    message = []
    for pedido in pedidos_natura:
        message.append(str(pedido))
    return message

if __name__ == "__main__":
    msg_pedidos_natura = nf_erro()
    send_message(msg_pedidos_natura, "nf-erro-natura", webhook_url=url)  # Send to a specific Google Chat space