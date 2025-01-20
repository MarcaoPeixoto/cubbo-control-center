from datetime import datetime, timedelta
import json
import operator
from collections import Counter
import numpy as np
from dateutil import parser
import requests

date_format="%Y-%m-%dT%H:%M:%S"
date_format2="%Y-%m-%dT%H:%M:%S.%f"

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
        return res.json()

    dataset = res.json()
    return dataset

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

class CONFIG:

    PERECIVEIS_ACCOUNTS = []
    JSON_CONFIG = []

    def __init__(self):
        self.JSON_CONFIG = self.get_configurations()
        self.PERECIVEIS_ACCOUNTS = self.JSON_CONFIG['pereciveis']

    def get_configurations(self):
        with open("marcas.json", "r", ) as f:
            config = json.load(f)
        return config

config = CONFIG()


def aguardando_coleta(transportadora):
    print(f"Received transportadora: {transportadora}")

    if not transportadora:
        return {"message": ["Transportadora nao especificada"], "files": []}

    transportadora = transportadora.upper().strip()
    
    print(f"Standardized transportadora: {transportadora}")

    if transportadora in ["JT", "JT EXPRESS"]:
        transportadora = "JT Express"
    elif transportadora in ["LOGGI"]:
        transportadora = "LOGGI"

    processado = datetime.now() - timedelta(days=2)
    processado = processado.replace(hour=10, minute=30, second=0)

    if transportadora == "LOGGI":
        processado = processado.replace(hour=17, minute=00, second=0)
    


    variaveis = process_data(
        {
            'dt_processado': processado,
            'transportadora': transportadora,
        })



    dataset = get_dataset('2754', variaveis)

    #print(dataset)
    
    sorted_data = []

    print(f"processado: {processado}")

    for orders in dataset:

        hoje = datetime.now()

        dt_inicio = hoje - timedelta(days=2)
        dt_fim = hoje - timedelta(days=1)
        dt_inicio = dt_inicio.replace(hour=14, minute=0, second=0)
        dt_fim = dt_fim.replace(hour=14, minute=0, second=0)

        if orders['carrier_name'] == "CORREIOS":
            dt_inicio = dt_inicio.replace(hour=11, minute=30, second=0)
            dt_fim = dt_fim.replace(hour=11, minute=30, second=0)
        
        if orders['timezone'] is not None and orders['timezone'] != "":
            parsed_date = parse_date(orders['timezone'])
            if parsed_date:
                orders['timezone'] = parsed_date
            else:
                continue  # Skip orders with unparseable dates

        if orders['carrier_name'].upper() != transportadora.upper():
            continue

        print(f"dt_inicio: {dt_inicio}")
        print(f"dt_fim: {dt_fim}")

        if orders['timezone'] < dt_inicio:
            continue
        if orders['timezone'] > dt_fim:
            continue



        sorted_data.append({
            'pedido': orders['order_number'],
            'rastreio': orders['shipping_number'],
            'loja': orders['stores__name'],
            'processado': orders['timezone'],
            'transportadora': orders['carrier_name']
        })


    if len(sorted_data) == 0:
        message = "Nenhum pedido aguardando coleta."
    else:

        message = "\nPedidos aguardando coleta: "+str(len(sorted_data))

        message += "\n\nPedido  |  Rastreio  |  Loja   |  Processado  |  Transportadora\n\n"

        for resposta in sorted_data:
            message += (f"{resposta['pedido']}  |  {resposta['rastreio']}  |  {resposta['loja']}  |  {resposta['processado']}  |  {resposta['transportadora']}\n")

    return {"message": [message], "files": []} 

def parse_date(date_str):
    if date_str is None or date_str == "":
        return None
    try:
        # First try parsing as ISO format with timezone
        return parser.parse(date_str).replace(tzinfo=None)
    except (ValueError, TypeError):
        try:
            # Then try the standard date format
            return datetime.strptime(date_str, date_format)
        except ValueError:
            try:
                # Finally try the alternate date format
                return datetime.strptime(date_str, date_format2)
            except ValueError as e:
                print(f"Warning: Unable to parse date: {date_str}, Error: {e}")
                return None 