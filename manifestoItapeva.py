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

# Replace the existing redis_client creation with:
redis_client = get_redis_connection()

# Replace the existing docs_service initialization with this:
docs_service = authenticate_google()

def get_manifesto_itapeva(carrier):
    try:
        current_date = datetime.now() - timedelta(hours=3)
        
        if carrier == "JT":
            carrier = "JT Express"

        # Convert dates to string format YYYY-MM-DD
        date_str = current_date.strftime('%Y-%m-%d')
        
        manifesto_inputs = {
            'carrier_name': carrier, 
            'shipping_date': date_str,
            'dispatch_date': date_str
        }
        
        # Debug prints
        print("Input parameters:")
        print(f"Carrier: {carrier}")
        print(f"Date: {date_str}")
        
        processed_params = process_data(manifesto_inputs)
        print("Processed parameters:")
        print(json.dumps(processed_params, indent=2))
        
        # Get the dataset with parameters - using dataset 9450 for Itapeva
        response = get_dataset('9450', processed_params)
        
        # Debug print the response
        print(f"Response type: {type(response)}")
        
        # Convert response to list if it's a dictionary
        if isinstance(response, dict):
            if 'data' in response:
                pedidos = response['data']
            else:
                pedidos = [response]  # Convert single dict to list
        else:
            pedidos = response

        if not pedidos:  # Add validation
            print(f"No orders found for carrier {carrier} on date {date_str}")
            raise ValueError("No orders found for the specified carrier and date")

        # Debug prints for data structure
        print("\nData Structure Analysis:")
        print(f"Pedidos type: {type(pedidos)}")

        # Validate required fields exist
        required_fields = ['shipping_number', 'dispatched_at']
        if pedidos and isinstance(pedidos[0], dict):
            missing_fields = [field for field in required_fields if field not in pedidos[0]]
            if missing_fields:
                print(f"Warning: Missing required fields: {missing_fields}")

        print(f"Total orders retrieved: {len(pedidos)}")
        print(f"Sample order format: {pedidos[0] if pedidos else 'No orders'}")

        # Filter out orders where 'shipping_number' starts with 'MEL'
        filtered_pedidos = [order for order in pedidos if order.get('shipping_number') is not None and not order.get('shipping_number', '').startswith('MEL')]

        # Now proceed with the filtered list
        trackings_dispatched = [x['shipping_number'] for x in filtered_pedidos if x.get('dispatched_at')]
        trackings_not_dispatched = [x['shipping_number'] for x in filtered_pedidos if not x.get('dispatched_at')]

        # Collect data to insert into Google Doc
        data = {
            'current_date': current_date,
            'carrier': carrier,
            'total_pedidos': len(pedidos),
            'not_dispatched_count': len(trackings_not_dispatched),
            'not_dispatched_trackings': trackings_not_dispatched,
            'dispatched_count': len(trackings_dispatched),
            'dispatched_trackings': trackings_dispatched
        }

        return data
    except Exception as e:
        print(f"Error in get_manifesto_itapeva: {str(e)}")
        # Only try to print details about pedidos if it exists
        pedidos_exists = 'pedidos' in locals() or 'pedidos' in globals()
        if pedidos_exists:
            print(f"Type of pedidos: {type(pedidos)}")  # Debug print
            if pedidos:
                print(f"Type of first item: {type(pedidos[0])}")  # Debug print
                print(f"First item content: {pedidos[0]}")  # Debug print
        else:
            print("Error occurred before pedidos variable was defined")
        raise

def nao_despachados_itapeva(data, transportadora):
    quantidade_nao_despachados = data['not_dispatched_count']

    warning_text = f"Transportadora: {transportadora}\n\n"
    warning_text += f"Pedidos já processados e bipados: {data['dispatched_count']}\n\n"

    if data['carrier'] in ["CORREIOS"]:
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

def save_to_google_docs_itapeva(document_title, data, folder_id=None, transportadora=None):
    if not data or not document_title:
        raise ValueError("Missing required data or document title")

    try:
        print("\nStarting document creation:")
        print(f"Title: {document_title}")
        print(f"Folder ID: {folder_id}")
        print(f"Transportadora: {transportadora}")

        # Create a new document
        if folder_id:
            drive_service = get_drive_service()
            file_metadata = {
                'name': document_title,
                'parents': [folder_id],
                'mimeType': 'application/vnd.google-apps.document'
            }
            
            print("\nCreating document in specified folder...")
            file = drive_service.files().create(
                body=file_metadata, 
                fields='id,parents,name'
            ).execute()
            
            document_id = file.get('id')
            print(f"Document created:")
            print(f"ID: {document_id}")
            print(f"Name: {file.get('name')}")
            print(f"Parent folders: {file.get('parents')}")
        else:
            print("Warning: No folder ID provided, creating in root")
            document = docs_service.documents().create(body={'title': document_title}).execute()
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
        manifesto_text = (f"ROMANEIO ITAPEVA\n\nData: {data['current_date']:%d/%m/%Y}\n"
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

        # Bold "ROMANEIO ITAPEVA"
        romaneio_index = manifesto_text.find("ROMANEIO ITAPEVA")
        romaneio_end = romaneio_index + len("ROMANEIO ITAPEVA")
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
        if transportadora in ["CORREIOS"]:
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
        print(f"Google API error: {err}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise

def link_docs_itapeva(transportadora):
    try:
        env_config = dotenv_values(".env")
        # Load folder IDs from environment variables with debug prints
        loggi_folder = env_config.get('LOGGI_FOLDER_MG_ID') or os.environ.get("LOGGI_FOLDER_MG_ID")
        correios_folder = env_config.get('CORREIOS_FOLDER_MG_ID') or os.environ.get("CORREIOS_FOLDER_MG_ID")
        jt_folder = env_config.get('JT_FOLDER_MG_ID') or os.environ.get("JT_FOLDER_MG_ID")

        print("\nFolder IDs from environment:")
        print(f"LOGGI Folder ID: {loggi_folder}")
        print(f"CORREIOS Folder ID: {correios_folder}")
        print(f"JT Folder ID: {jt_folder}")

        # Determine the correct folder ID based on the transportadora
        folder_id = None
        if transportadora == "LOGGI":
            folder_id = loggi_folder
        elif transportadora == "CORREIOS":
            folder_id = correios_folder
        elif transportadora == "JT":
            folder_id = jt_folder

        if not folder_id:
            print(f"Error: No folder ID found for transportadora: {transportadora}")
            return None

        print(f"\nSelected folder details:")
        print(f"Transportadora: {transportadora}")
        print(f"Using folder ID: {folder_id}")

        # Verify folder exists before proceeding
        try:
            drive_service = get_drive_service()
            folder = drive_service.files().get(fileId=folder_id).execute()
            print(f"Verified folder name: {folder.get('name')}")
        except Exception as e:
            print(f"Error verifying folder: {str(e)}")
            return None

        # Get manifesto data
        data = get_manifesto_itapeva(transportadora)
        current_date = datetime.now() - timedelta(hours=3)
        document_title = f'Manifesto Itapeva {transportadora} {current_date:%d/%m/%Y}'

        print(f"\nCreating document:")
        print(f"Title: {document_title}")
        print(f"Date: {current_date:%d/%m/%Y}")
        
        # Create the document
        document_id = save_to_google_docs_itapeva(document_title, data, folder_id, transportadora)
        
        if document_id:
            doc_url = f'https://docs.google.com/document/d/{document_id}/edit'
            print(f"\nSuccess:")
            print(f"Document created successfully: {doc_url}")
            print(f"In folder: {folder.get('name')} ({folder_id})")
            return doc_url
        else:
            print("\nError: Failed to create document")
            return None

    except Exception as e:
        print(f"\nError in link_docs_itapeva: {str(e)}")
        raise 