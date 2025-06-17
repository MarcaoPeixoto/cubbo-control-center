import requests
import json
from datetime import datetime
from slack_bot_interface import send_message

def create_metabase_token():
    url = 'https://cubbo.metabaseapp.com/api/session'
    data = {
        'username': "marco.peixoto@cubbo.com",
        'password': "KeffE2qvh3htUEa@!"
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

    res = requests.post(METABASE_ENDPOINT + '/api/card/' + question + '/query/json',
                        headers={"Content-Type": "application/json",
                                 'X-Metabase-Session': METABASE_TOKEN},
                        json=params)
    if res.status_code != 200:
        print(f'Failed to get dataset: {res.content}')
        return None  # Return None instead of res.json() on failure

    dataset = res.json()
    return dataset

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
    send_message(msg_pedidos_natura, "teste-bot-marco")  # Send to a specific channel