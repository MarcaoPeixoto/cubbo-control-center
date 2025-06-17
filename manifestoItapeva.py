from datetime import datetime, timedelta
import os
import json
from dotenv import dotenv_values
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from redis_connection import get_redis_connection
import requests
from metabase import get_dataset, process_data
from google_auth import get_docs_service, get_drive_service
from google_auth import authenticate_google
from google.oauth2 import service_account
import time
from tenacity import retry, stop_after_attempt, wait_exponential

#para atualizar
# Replace the existing redis_client creation with:
redis_client = get_redis_connection()

# Replace the existing docs_service initialization with this:
docs_service = authenticate_google()

# Load environment variables
env_config = dotenv_values(".env")

# Load folder IDs from environment variables with debug prints
jt_folder = env_config.get('JT_FOLDER_MG_ID') or os.environ.get("JT_FOLDER_MG_ID")
loggi_folder = env_config.get('LOGGI_FOLDER_MG_ID') or os.environ.get("LOGGI_FOLDER_MG_ID")
correios_folder = env_config.get('CORREIOS_FOLDER_MG_ID') or os.environ.get("CORREIOS_FOLDER_MG_ID")

# Debug prints for folder IDs
print("Loaded folder IDs:")
print(f"JT Folder ID: {jt_folder}")
print(f"LOGGI Folder ID: {loggi_folder}")
print(f"CORREIOS Folder ID: {correios_folder}")

def get_manifesto_itapeva(carrier):
    try:
        print("Input parameters:")
        print(f"Carrier: {carrier}")
        
        # Get current date
        current_date = datetime.now()
        print(f"Date: {current_date:%Y-%m-%d}")
        
        # Prepare parameters for Metabase query
        params = {
            'carrier_name': carrier,
            'shipping_date': current_date,
            'dispatch_date': current_date
        }
        
        # Process parameters
        processed_params = process_data(params)
        print("Processed parameters:")
        print(json.dumps(processed_params, indent=2))
        
        # Get data from Metabase
        response = get_dataset('9450', processed_params)
        
        # Debug prints for response
        print("Response type:", type(response))
        print("\nData Structure Analysis:")
        
        # Convert response to list if it's a dictionary
        if isinstance(response, dict):
            if 'data' in response:
                pedidos = response['data']
            else:
                pedidos = [response]  # Convert single dict to list
        else:
            pedidos = response
            
        if not pedidos:
            print(f"No orders found for carrier {carrier} on date {current_date:%Y-%m-%d}")
            raise ValueError("No orders found for the specified carrier and date")
            
        print(f"Pedidos type: {type(pedidos)}")
        print(f"Total orders retrieved: {len(pedidos)}")
        if pedidos:
            print(f"Sample order format: {pedidos[0]}")
        
        # Process orders
        trackings_dispatched = [x['shipping_number'] for x in pedidos if x.get('dispatched_at')]
        trackings_not_dispatched = [x['shipping_number'] for x in pedidos if not x.get('dispatched_at')]
        
        # Collect data to return
        data = {
            'current_date': current_date,
            'carrier': carrier,
            'total_pedidos': len(pedidos),
            'not_dispatched_count': len(trackings_not_dispatched),
            'not_dispatched_trackings': trackings_not_dispatched,
            'dispatched_count': len(trackings_dispatched),
            'dispatched_trackings': trackings_dispatched,
            'pedidos': pedidos  # Include the full pedidos list
        }
        
        return data
        
    except Exception as e:
        print(f"Error in get_manifesto_itapeva: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise

def nao_despachados_itapeva(data, transportadora):
    quantidade_nao_despachados = data['not_dispatched_count']

    warning_text = f"Transportadora: {transportadora}\n\n"
    warning_text += f"Pedidos já processados e bipados: {data['dispatched_count']}\n\n"

    if data['carrier'] in ["Mercado Envíos", "CORREIOS"]:
        if quantidade_nao_despachados > 0:
            warning_text += f"<p><strong>ATENÇÃO! {quantidade_nao_despachados} pedidos processados e não despachados:</strong></p>"
            warning_text += "<p>Não foram despachados os seguintes pedidos:</p>"
            warning_text += "<ul>"
            warning_text += "".join([f"<li>{str(item) if item else 'ERRO'}</li>" for item in data['not_dispatched_trackings']])
            warning_text += "</ul>"
        elif quantidade_nao_despachados == 1:
            warning_text += f"<p><strong>ATENÇÃO! {quantidade_nao_despachados} pedido processado e não despachado:</strong></p>"
            warning_text += "<p>Não foi despachado o seguinte pedido:</p>"
            warning_text += "<ul>"
            warning_text += "".join([f"<li>{str(item) if item else 'ERRO'}</li>" for item in data['not_dispatched_trackings']])
            warning_text += "</ul>"
        else:
            warning_text += "<p>Todos os pedidos foram despachados!</p>"
    else:
        if quantidade_nao_despachados > 0:
            warning_text += f"<p><strong>ATENÇÃO! {quantidade_nao_despachados} pedidos processados e não despachados:</strong></p>"
            warning_text += "<p>Não foram despachados os seguintes pedidos:</p>"
            warning_text += "<ul>"
            warning_text += "".join([f"<li>{str(item) if item else 'ERRO'}</li>" for item in data['not_dispatched_trackings']])
            warning_text += "</ul>"
        elif quantidade_nao_despachados == 1:
            warning_text += f"<p><strong>ATENÇÃO! {quantidade_nao_despachados} pedido processado e não despachado:</strong></p>"
            warning_text += "<p>Não foi despachado o seguinte pedido:</p>"
            warning_text += "<ul>"
            warning_text += "".join([f"<li>{str(item) if item else 'ERRO'}</li>" for item in data['not_dispatched_trackings']])
            warning_text += "</ul>"
        else:
            warning_text += "<p>Todos os pedidos foram despachados!</p>"

    return warning_text

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def save_to_google_docs_itapeva(data, carrier):
    try:
        print("\nStarting save_to_google_docs_itapeva")
        print("Input data type:", type(data))
        print("Input data structure:", json.dumps(data, indent=2) if isinstance(data, (list, dict)) else str(data))
        
        # Validate input data
        if not isinstance(data, dict):
            raise ValueError(f"Expected dictionary data, got {type(data)}")
        
        # Get current date
        current_date = datetime.now()
        date_str = current_date.strftime('%d/%m/%Y')
        
        # Get folder ID based on carrier
        folder_id = link_docs_itapeva(carrier)
        if not folder_id:
            raise ValueError(f"No folder ID found for carrier: {carrier}")
        
        # Create document title
        document_title = f"Manifesto {carrier} {date_str}"
        print(f"Creating document with title: {document_title}")
        
        # Create document
        document = docs_service.documents().create(body={'title': document_title}).execute()
        document_id = document.get('documentId')
        print(f"Document created with ID: {document_id}")
        
        # Add delay to respect rate limits
        time.sleep(1)
        
        # Prepare document content
        requests = [
            {
                'insertText': {
                    'location': {'index': 1},
                    'text': f"Manifesto {carrier} - {date_str}\n\n"
                }
            }
        ]
        
        # Add orders to document
        for order in data.get('pedidos', []):
            try:
                if not isinstance(order, dict):
                    print(f"Warning: Skipping invalid order format: {order}")
                    continue
                
                # Format dates safely
                created_at = order.get('created_at', '')
                dispatched_at = order.get('dispatched_at', '')
                
                if created_at:
                    try:
                        if isinstance(created_at, str):
                            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00')).strftime('%d/%m/%Y %H:%M')
                        else:
                            created_at = str(created_at)
                    except Exception as e:
                        print(f"Error formatting created_at: {e}")
                        created_at = str(created_at)
                
                if dispatched_at:
                    try:
                        if isinstance(dispatched_at, str):
                            dispatched_at = datetime.fromisoformat(dispatched_at.replace('Z', '+00:00')).strftime('%d/%m/%Y %H:%M')
                        else:
                            dispatched_at = str(dispatched_at)
                    except Exception as e:
                        print(f"Error formatting dispatched_at: {e}")
                        dispatched_at = str(dispatched_at)
                
                # Format order row with safe string conversion
                order_row = (
                    f"Pedido: {str(order.get('order_number', ''))}\n"
                    f"Cubbo ID: {str(order.get('cubbo_id', ''))}\n"
                    f"Nome: {str(order.get('name', ''))}\n"
                    f"Data de Criação: {created_at}\n"
                    f"Data de Despacho: {dispatched_at}\n"
                    f"Transportadora: {str(order.get('carrier_name', ''))}\n\n"
                )
                
                requests.append({
                    'insertText': {
                        'location': {'index': 1},
                        'text': order_row
                    }
                })
                
            except Exception as e:
                print(f"Error processing order: {str(e)}")
                continue
        
        if not requests:
            raise ValueError("No valid orders to add to document")
        
        # Execute batch update with rate limiting
        print("Executing batch update...")
        result = docs_service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': requests}
        ).execute()
        print("Batch update completed")
        
        # Add delay before moving file
        time.sleep(1)
        
        # Move document to appropriate folder
        print(f"Moving document to folder: {folder_id}")
        file = docs_service.files().update(
            fileId=document_id,
            addParents=folder_id,
            fields='id, parents'
        ).execute()
        print("Document moved successfully")
        
        # Generate document URL
        document_url = f"https://docs.google.com/document/d/{document_id}/edit"
        print(f"Document URL: {document_url}")
        
        return document_url
        
    except HttpError as error:
        if error.resp.status == 429:
            print("Rate limit exceeded. Waiting before retry...")
            time.sleep(60)  # Wait for 1 minute before retrying
            raise  # This will trigger the retry decorator
        else:
            print(f"Error in save_to_google_docs_itapeva: {str(error)}")
            raise
    except Exception as e:
        print(f"Error in save_to_google_docs_itapeva: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise

def link_docs_itapeva(transportadora):
    # Define folder IDs for each carrier
    folder_ids = {
        'LOGGI': loggi_folder,
        'CORREIOS': correios_folder,
        'JT': jt_folder
    }
    
    # Get the folder ID for the specified carrier
    folder_id = folder_ids.get(transportadora)
    if not folder_id:
        raise ValueError(f"No folder ID configured for carrier: {transportadora}")
    
    # Validate folder ID format
    if not folder_id or len(folder_id) < 10:  # Basic validation
        raise ValueError(f"Invalid folder ID format for carrier {transportadora}: {folder_id}")
    
    print(f"Using folder ID for {transportadora}: {folder_id}")
    return folder_id

def get_difal_order_ids():
    pedidos_difal = get_dataset('613')
    return [d['Orders → ID'] for d in pedidos_difal if 'Orders → ID' in d]

""" # Move this to a if __name__ == "__main__": block
if __name__ == "__main__":
    try:
        result = get_manifesto("LOGGI")
        print("Manifesto generated successfully")
    except Exception as e:
        print(f"Failed to generate manifesto: {e}") """