import os
import json
from dotenv import dotenv_values
import requests
from datetime import datetime

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
    
def get_dataset(question, params={}):
    METABASE_ENDPOINT = "https://cubbo.metabaseapp.com"
    METABASE_TOKEN = create_metabase_token()

    # Debug print to verify the request
    print(f"Making request to question {question} with parameters: {params}")

    try:
        res = requests.post(
            f"{METABASE_ENDPOINT}/api/card/{question}/query/json",
            headers={
                "Content-Type": "application/json",
                'X-Metabase-Session': METABASE_TOKEN
            },
            json=params,  # Use json parameter instead of params
            timeout=30  # Add timeout
        )
        
        # Debug response
        print(f"Response status code: {res.status_code}")
        
        if res.status_code != 200:
            print(f"Error response from Metabase: {res.text}")
            raise Exception(f"Metabase query failed with status {res.status_code}")
            
        dataset = res.json()
        return dataset

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        raise

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
