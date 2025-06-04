import os
import json
from dotenv import dotenv_values
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

def create_metabase_token():

    env_config = dotenv_values(".env")
    metabase_user = env_config.get('METABASE_USER')
    
    if metabase_user is not None:
        metabase_password = env_config.get('METABASE_PASSWORD')
    else:
        metabase_user = os.environ["METABASE_USER"]
        metabase_password = os.environ["METABASE_PASSWORD"]

    url = 'https://cubbo.metabaseapp.com/api/session'
    data = {
        'username': metabase_user,
        'password': metabase_password
    }

    headers = {'Content-Type': 'application/json'}
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        return response.json().get('id')
    else:
        raise Exception(f'Failed to create token: {response.content}')
    
def create_session_with_retries():
    session = requests.Session()
    retries = Retry(
        total=3,  # number of retries
        backoff_factor=1,  # wait 1, 2, 4 seconds between retries
        status_forcelist=[500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def get_dataset(question, params={}):
    METABASE_ENDPOINT = "https://cubbo.metabaseapp.com"
    attempt = 0
    
    while True:  # Keep trying indefinitely
        try:
            attempt += 1
            METABASE_TOKEN = create_metabase_token()
            session = create_session_with_retries()
            
            print(f"Attempt {attempt} to fetch data from Metabase...")
            
            res = session.post(
                f"{METABASE_ENDPOINT}/api/card/{question}/query/json",
                headers={
                    "Content-Type": "application/json",
                    'X-Metabase-Session': METABASE_TOKEN
                },
                json=params,
                timeout=(30, 90)  # (connect timeout, read timeout)
            )
            
            if res.status_code == 200:
                dataset = res.json()
                print("Successfully fetched data from Metabase")
                return dataset
            else:
                print(f"Error response from Metabase: {res.text}")
                print(f"Retrying in {min(attempt * 2, 30)} seconds...")  # Cap wait time at 30 seconds
                time.sleep(min(attempt * 2, 30))

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"Network error on attempt {attempt}: {str(e)}")
            print(f"Retrying in {min(attempt * 2, 30)} seconds...")  # Cap wait time at 30 seconds
            time.sleep(min(attempt * 2, 30))
            
        except Exception as e:
            print(f"Unexpected error on attempt {attempt}: {str(e)}")
            print(f"Retrying in {min(attempt * 2, 30)} seconds...")  # Cap wait time at 30 seconds
            time.sleep(min(attempt * 2, 30))

def process_data(inputs):

    def create_param(tag, param_value):
        param = {}
        if type(param_value) == int:
            param['type'] = "number/="
            param['value'] = param_value
        elif type(param_value) == datetime:
            param['type'] = "date/single"
            param['value'] = f"{param_value:%Y-%m-%d}"
        else:
            param['type'] = "category"
            param['value'] = param_value

        param['target'] = ["variable", ["template-tag", tag]]
        return param

    params = []
    for input_name, input_value in inputs.items():
        if input_value is not None:
            param = create_param(input_name, input_value)
            params.append(param)

    return {'parameters': json.dumps(params)}

print(create_metabase_token())