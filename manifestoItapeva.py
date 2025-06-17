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
        print("Pedidos type:", type(response))
        print("Total orders retrieved:", len(response))
        if response:
            print("Sample order format:", response[0])
        
        # Validate and process the response
        if not isinstance(response, list):
            raise ValueError(f"Expected list response from Metabase, got {type(response)}")
        
        # Process each order to ensure required fields
        processed_orders = []
        for order in response:
            if not isinstance(order, dict):
                print(f"Warning: Skipping invalid order format: {order}")
                continue
                
            processed_order = {
                'order_number': str(order.get('order_number', '')),
                'cubbo_id': str(order.get('cubbo_id', '')),
                'name': str(order.get('name', '')),
                'created_at': order.get('created_at', ''),
                'dispatched_at': order.get('dispatched_at', ''),
                'carrier_name': str(order.get('carrier_name', ''))
            }
            processed_orders.append(processed_order)
        
        return processed_orders
        
    except Exception as e:
        print(f"Error in get_manifesto_itapeva: {str(e)}")
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

def save_to_google_docs_itapeva(title, pedidos, transportadora):
    try:
        print("\nStarting document creation:")
        print(f"Title: {title}")
        print(f"Number of orders: {len(pedidos)}")
        print("Sample order data:", pedidos[0] if pedidos else "No orders")
        
        # Get folder ID
        folder_id = link_docs_itapeva(transportadora)
        print(f"Folder ID: {folder_id}")
        print(f"Transportadora: {transportadora}")
        
        # Create document
        print("\nCreating document in specified folder...")
        docs_service = authenticate_google()
        drive_service = get_drive_service()
        
        # Create empty document
        document = docs_service.documents().create(body={'title': title}).execute()
        document_id = document.get('documentId')
        
        # Initialize document with a paragraph
        docs_service.documents().batchUpdate(
            documentId=document_id,
            body={
                'requests': [
                    {
                        'insertText': {
                            'location': {
                                'index': 1
                            },
                            'text': '\n'
                        }
                    }
                ]
            }
        ).execute()
        
        # Add title
        docs_service.documents().batchUpdate(
            documentId=document_id,
            body={
                'requests': [
                    {
                        'insertText': {
                            'location': {
                                'index': 1
                            },
                            'text': f'Manifesto {transportadora} - {datetime.now().strftime("%d/%m/%Y")}\n\n'
                        }
                    }
                ]
            }
        ).execute()
        
        # Add table header
        docs_service.documents().batchUpdate(
            documentId=document_id,
            body={
                'requests': [
                    {
                        'insertText': {
                            'location': {
                                'index': 1
                            },
                            'text': 'Pedido\tCubbo ID\tNome\tData de Criação\tData de Despacho\n'
                        }
                    }
                ]
            }
        ).execute()
        
        # Add orders
        for pedido in pedidos:
            # Format dates if they exist
            created_at = pedido.get('created_at', '')
            if created_at:
                try:
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00')).strftime('%d/%m/%Y %H:%M:%S')
                except:
                    created_at = str(created_at)
            
            dispatched_at = pedido.get('dispatched_at', '')
            if dispatched_at:
                try:
                    dispatched_at = datetime.fromisoformat(dispatched_at.replace('Z', '+00:00')).strftime('%d/%m/%Y %H:%M:%S')
                except:
                    dispatched_at = str(dispatched_at)
            
            # Create order row
            order_row = f"{pedido.get('order_number', '')}\t{pedido.get('cubbo_id', '')}\t{pedido.get('name', '')}\t{created_at}\t{dispatched_at}\n"
            
            docs_service.documents().batchUpdate(
                documentId=document_id,
                body={
                    'requests': [
                        {
                            'insertText': {
                                'location': {
                                    'index': 1
                                },
                                'text': order_row
                            }
                        }
                    ]
                }
            ).execute()
        
        # Move document to specified folder
        file = drive_service.files().get(fileId=document_id, fields='id, parents').execute()
        previous_parents = ",".join(file.get('parents', []))
        
        drive_service.files().update(
            fileId=document_id,
            addParents=folder_id,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        
        # Get document URL
        document_url = f"https://docs.google.com/document/d/{document_id}/edit"
        print(f"Document created successfully: {document_url}")
        return document_url
        
    except Exception as e:
        print(f"Error in save_to_google_docs_itapeva: {e}")
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