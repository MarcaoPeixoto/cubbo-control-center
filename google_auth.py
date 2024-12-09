from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from redis_connection import get_redis_connection
import json

redis_client = get_redis_connection()

def authenticate_google(service_type='docs', additional_scopes=None):
    """
    Authenticate with Google APIs and return the service object.
    
    Args:
        service_type (str): The type of service to build ('docs', 'drive', 'sheets')
        additional_scopes (list): Optional additional scopes to add to the base scopes
    
    Returns:
        googleapiclient.discovery.Resource: The authenticated service object
    """
    BASE_SCOPES = [
        'https://www.googleapis.com/auth/documents.readonly',
        'https://www.googleapis.com/auth/drive.file',
        'https://www.googleapis.com/auth/drive'
    ]
    
    SCOPES = BASE_SCOPES + (additional_scopes or [])
    
    creds = None
    token_json = redis_client.get('token_json')

    if token_json:
        creds = Credentials.from_authorized_user_info(json.loads(token_json), SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing credentials: {e}")
                creds = None
        
        if not creds:
            credentials_json = redis_client.get('credentials_json')
            if not credentials_json:
                raise Exception("credentials.json not found in Redis")
            flow = InstalledAppFlow.from_client_config(
                json.loads(credentials_json), SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Update token in Redis
        redis_client.set('token_json', creds.to_json())

    # Build the appropriate service based on service_type
    service_versions = {
        'docs': 'v1',
        'drive': 'v3',
        'sheets': 'v4'
    }
    
    version = service_versions.get(service_type, 'v1')
    return build(service_type, version, credentials=creds)

# Helper functions for common service combinations
def get_docs_service(additional_scopes=None):
    return authenticate_google('docs', additional_scopes)

def get_drive_service(additional_scopes=None):
    return authenticate_google('drive', additional_scopes)

def get_sheets_service(additional_scopes=None):
    return authenticate_google('sheets', additional_scopes) 