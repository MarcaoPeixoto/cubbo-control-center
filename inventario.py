from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from redis_connection import get_redis_connection
from google.auth.exceptions import RefreshError
from metabase import get_dataset, process_data
from google_auth import authenticate_google
from dotenv import dotenv_values

env_config = dotenv_values(".env")

redis_client = get_redis_connection()

drive_service = authenticate_google('drive')


def get_estoque(bin):
    try:
        # Use process_data to format parameters correctly for Metabase
        params = process_data({'bin': bin})

        estoque = get_dataset('9845', params)
        
        # Ensure we return a list
        if estoque is None:
            return []
        elif isinstance(estoque, list):
            return estoque
        elif isinstance(estoque, dict) and 'error' in estoque:
            print(f"Error from Metabase: {estoque['error']}")
            return []
        else:
            print(f"Unexpected response type from Metabase: {type(estoque)}")
            return []
            
    except Exception as e:
        print(f"Error in get_estoque: {str(e)}")
        return []
   