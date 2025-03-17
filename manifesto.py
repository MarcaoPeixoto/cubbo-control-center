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

#para atualizar
# Replace the existing redis_client creation with:
redis_client = get_redis_connection()


# Replace the existing docs_service initialization with this:
docs_service = authenticate_google()


def get_manifesto(carrier):
    # Get current date (with -3 hours adjustment)
    current_date = datetime.now() - timedelta(hours=3)
    formatted_date = current_date.strftime('%Y-%m-%d')
    print(f"Current date being used: {formatted_date}")

    # Map carrier names exactly as they appear in the database
    carrier_mapping = {
        "MELI": "Mercado Envíos",
        "JT": "JT Express",
        "CORREIOS": "CORREIOS",  # Make sure this matches exactly
        "LOGGI": "LOGGI",        # Make sure this matches exactly
        "IMILE": "IMILE"         # Make sure this matches exactly
    }

    if carrier not in carrier_mapping:
        raise ValueError(f"Unknown carrier: {carrier}")
    
    carrier_name = carrier_mapping[carrier]
    print(f"Using carrier name: {carrier_name}")  # Debug print

    manifesto_inputs = {
        'carrier_name': carrier_name,
        'shipping_date': formatted_date,
        'dispatch_date': formatted_date
    }
    print(f"Query parameters: {manifesto_inputs}")
    
    manifesto_inputs = process_data(manifesto_inputs)
    print(f"Processed parameters: {manifesto_inputs}")  # Debug print
    
    pedidos = get_dataset('578', manifesto_inputs)
    print(f"Total orders from initial dataset: {len(pedidos)}")
    
    # Double check carrier name in results
    if pedidos:
        unique_carriers = set(order.get('carrier_name') for order in pedidos)
        print(f"Carriers found in results: {unique_carriers}")
    
    # Filter by both dispatch date and carrier
    pedidos = [
        order for order in pedidos 
        if (
            order.get('dispatched_at') and 
            str(order.get('dispatched_at')).startswith(formatted_date) and
            order.get('carrier_name') == carrier_name  # Use exact carrier name
        )
    ]
    print(f"Orders after date and carrier filtering: {len(pedidos)}")
    
    # Rest of the filtering
    pedidos_difal = get_difal_order_ids()
    print(f"Number of DIFAL orders to exclude: {len(pedidos_difal)}")
    pedidos = [item for item in pedidos if item.get('cubbo_id') not in pedidos_difal]
    print(f"Orders after DIFAL filtering: {len(pedidos)}")
    
    # Print a sample order to debug date formats
    if pedidos:
        print("Sample order dates:")
        print(f"shipping_date: {pedidos[0].get('shipping_date')}")
        print(f"dispatched_at: {pedidos[0].get('dispatched_at')}")
    
    # Filter out orders where 'shipping_number' starts with 'MEL'
    filtered_pedidos = [order for order in pedidos if order.get('shipping_number') is not None and not order.get('shipping_number', '').startswith('MEL')]
    print(f"Final filtered orders count: {len(filtered_pedidos)}")
    
    # Add sample data print to check the structure
    if filtered_pedidos:
        print(f"Sample order data structure: {filtered_pedidos[0]}")

    # Now proceed with the filtered list
    trackings_dispatched = [x['shipping_number'] for x in filtered_pedidos if x.get('dispatched_at')]
    trackings_not_dispatched = [x['shipping_number'] for x in filtered_pedidos if not x.get('dispatched_at')]

    # Collect data to insert into Google Doc
    data = {
        'current_date': formatted_date,
        'carrier': carrier_name,
        'total_pedidos': len(pedidos),
        'not_dispatched_count': len(trackings_not_dispatched),
        'not_dispatched_trackings': trackings_not_dispatched,
        'dispatched_count': len(trackings_dispatched),
        'dispatched_trackings': trackings_dispatched
    }

    return data

transportadora = "LOGGI"

def nao_despachados(data, transportadora):
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


def save_to_google_docs(document_title, data, folder_id=None):
    #colocar a transportadora em maiusculo aqui!
    try:

        # Create a new document
        document = {
            'title': document_title
        }
        
        # If a folder_id is provided, create the document in that folder
        if folder_id:
            drive_service = get_drive_service()
            file_metadata = {
                'name': document_title,
                'parents': [folder_id],
                'mimeType': 'application/vnd.google-apps.document'
            }
            file = drive_service.files().create(body=file_metadata, fields='id').execute()
            document_id = file.get('id')
        else:
            # Create the document in the root folder
            document = docs_service.documents().create(body=document).execute()
            document_id = document.get('documentId')

        print(f"Document created with ID: {document_id}")
        print(f"Folder ID used: {folder_id}")

        requests_body = []

        # Set document margins to 0.5 inches (36 points)
        requests_body.append({
            'updateDocumentStyle': {
                'documentStyle': {
                    'marginTop': {'magnitude': 36, 'unit': 'PT'},
                    'marginBottom': {'magnitude': 36, 'unit': 'PT'},
                    'marginLeft': {'magnitude': 36, 'unit': 'PT'},
                    'marginRight': {'magnitude': 36, 'unit': 'PT'},
                },
                'fields': 'marginTop,marginBottom,marginLeft,marginRight'
            }
        })

        # Create a header
        requests_body.append({
            'createHeader': {
                'type': 'DEFAULT',
                'sectionBreakLocation': {
                    'segmentId': '',
                    'index': 0
                }
            }
        })

        # Define the header ID (the API will assign it; we'll retrieve it later)
        # Since we can't get the headerId before executing the requests, we'll need to execute the first batchUpdate, get the headerId, then proceed
        # So we'll first execute the createHeader request, then get the headerId

        # Execute the first batch update to create the header and set margins
        result = docs_service.documents().batchUpdate(
            documentId=document_id, body={'requests': requests_body}).execute()

        # Get the header ID from the response
        header_id = None
        for reply in result.get('replies', []):
            if 'createHeader' in reply:
                header_id = reply['createHeader']['headerId']
                break

        if header_id is None:
            print("Failed to create header.")
            return None

        # Now prepare requests to insert text into the header
        header_requests = []

        # Prepare the manifesto_text
        manifesto_text = (f"ROMANEIO\n\nData: {data['current_date']}\n"
                          f"Transportadora: {data['carrier']}\nQuantidade: {data['total_pedidos']}\n\n")

        # Insert the manifesto_text into the header
        header_requests.append({
            'insertText': {
                'location': {
                    'segmentId': header_id,
                    'index': 0
                },
                'text': manifesto_text
            }
        })

        # Apply formatting to the header text (font size, alignment, bold for "ROMANEIO")
        header_requests.append({
            'updateTextStyle': {
                'range': {
                    'segmentId': header_id,
                    'startIndex': 0,
                    'endIndex': len(manifesto_text)
                },
                'textStyle': {
                    'fontSize': {'magnitude': 9, 'unit': 'PT'}
                },
                'fields': 'fontSize'
            }
        })

        # Apply justified alignment to the header paragraph(s)
        header_requests.append({
            'updateParagraphStyle': {
                'range': {
                    'segmentId': header_id,
                    'startIndex': 0,
                    'endIndex': len(manifesto_text)
                },
                'paragraphStyle': {
                    'alignment': 'JUSTIFIED'
                },
                'fields': 'alignment'
            }
        })

        # Bold "ROMANEIO"
        romaneio_index = manifesto_text.find("ROMANEIO")
        romaneio_end = romaneio_index + len("ROMANEIO")
        header_requests.append({
            'updateTextStyle': {
                'range': {
                    'segmentId': header_id,
                    'startIndex': romaneio_index,
                    'endIndex': romaneio_end
                },
                'textStyle': {
                    'bold': True
                },
                'fields': 'bold'
            }
        })

        # Now prepare the main body content
        body_requests = []

        index = 1  # Start at index 1 (after the document start)

        # Prepare content to insert
        content = ""

        # Add warning about not dispatched orders if any
        if transportadora == "MELI" or transportadora == "CORREIOS":
            content += "\t\t".join(data['dispatched_trackings'])
            content += "\n\n\nAssinatura Transportadora:\n\nAssinatura Cubbo:"
        else:
            content += "\t".join(data['dispatched_trackings'])
            content += "\n\n\nAssinatura Transportadora:\n\nAssinatura Cubbo:"

        # Insert content into the document body only if it's not empty
        if content:
            body_requests.append({
                'insertText': {
                    'location': {'index': index},
                    'text': content
                }
            })

            # Apply 9 pt font size and justified alignment to all content
            content_end_index = index + len(content)

            # Update text style (font size)
            body_requests.append({
                'updateTextStyle': {
                    'range': {
                        'startIndex': index,
                        'endIndex': content_end_index
                    },
                    'textStyle': {
                        'fontSize': {'magnitude': 9, 'unit': 'PT'}
                    },
                    'fields': 'fontSize'
                }
            })

            # Update paragraph style (justified alignment)
            body_requests.append({
                'updateParagraphStyle': {
                    'range': {
                        'startIndex': index,
                        'endIndex': content_end_index
                    },
                    'paragraphStyle': {
                        'alignment': 'JUSTIFIED'
                    },
                    'fields': 'alignment'
                }
            })

        # Now execute the header and body requests
        all_requests = header_requests + body_requests

        docs_service.documents().batchUpdate(
            documentId=document_id, body={'requests': all_requests}).execute()

        return document_id
    except HttpError as err:
        print(f"An error occurred: {err}")
        return None

def link_docs(transportadora):
    
    env_config = dotenv_values(".env")
    # Load folder IDs from environment variables
    loggi_folder = env_config.get('LOGGI_FOLDER_ID') or os.environ["LOGGI_FOLDER_ID"]
    meli_folder = env_config.get('MELI_FOLDER_ID') or os.environ["MELI_FOLDER_ID"]
    correios_folder = env_config.get('CORREIOS_FOLDER_ID') or os.environ["CORREIOS_FOLDER_ID"]
    imile_folder = env_config.get('IMILE_FOLDER_ID') or os.environ["IMILE_FOLDER_ID"]

    # Determine the correct folder ID based on the transportadora
    if transportadora == "LOGGI":
        folder_id = loggi_folder
    elif transportadora == "MELI":
        folder_id = meli_folder
    elif transportadora == "CORREIOS":
        folder_id = correios_folder
    elif transportadora == "IMILE":
        folder_id = imile_folder
    else:
        print(f"Unknown transportadora: {transportadora}")
        return None

    print(f"Using folder ID: {folder_id}")
    
    data = get_manifesto(transportadora)
    current_date = datetime.now() - timedelta(hours=3)
    document_title = f'Manifesto {transportadora} {current_date:%d/%m/%Y}'

    print(f"Attempting to create document: {document_title}")
    document_id = save_to_google_docs(document_title, data, folder_id)
    if document_id:
        doc_url = f'https://docs.google.com/document/d/{document_id}/edit'
        print(f"Document created successfully: {doc_url}")
        return doc_url
    else:
        print("Failed to create document")
        return None
    

def get_difal_order_ids():
    pedidos_difal = get_dataset('613')
    return [d['Orders → ID'] for d in pedidos_difal if 'Orders → ID' in d]

#get_manifesto("CORREIOS")