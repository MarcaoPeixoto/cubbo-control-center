import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def send_message_to_chat(message, space_name=None, webhook_url=None):
    """
    Send a message to Google Chat using webhook URL.
    
    Args:
        message (list): List of message strings to send
        space_name (str): Name of the Google Chat space (for logging purposes)
        webhook_url (str): Webhook URL for the Google Chat space
    """
    text = "\n".join(message)
    
    if not webhook_url:
        print("Error: webhook_url is required")
        return
    
    # Send message via webhook
    payload = {
        "text": text
    }
    
    try:
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            print(f"Message sent successfully via webhook to {space_name or 'webhook'}")
        else:
            print(f"Failed to send message via webhook. Status: {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Error sending message via webhook: {e}")

def send_message(message, space_name=None, webhook_url=None):
    """
    Convenience function to maintain compatibility with existing code.
    
    Args:
        message (list): List of message strings to send
        space_name (str): Name of the Google Chat space or webhook identifier
        webhook_url (str): Optional webhook URL for direct webhook usage
    """
    # If no webhook_url provided, try to get it from environment variable
    if webhook_url is None:
        webhook_url = os.getenv('NF_ERRO_NATURA_URL')
    
    send_message_to_chat(message, space_name, webhook_url) 