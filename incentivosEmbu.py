import requests
import json
from datetime import datetime, timedelta
import numpy as np
import os
from dotenv import dotenv_values
import redis

# Setting up datetime format
hora_agora = datetime.now()
nova_hora = (hora_agora - timedelta(hours=3)).strftime("%H:%M")

date_format = "%d-%m-%Y, %H:%M:%S"
date_format2 = "%d-%m-%Y, %H:%M:%S.%f"

# Loading environment variables
env_config = dotenv_values(".env")
redis_end = env_config.get('REDIS_END')
redis_port = env_config.get('REDIS_PORT')

# Initializing Redis client
redis_password = env_config.get('REDIS_PASSWORD')  # Add this line to get the password from your .env file
redis_client = redis.StrictRedis(host=redis_end, port=redis_port, password=redis_password, db=0, decode_responses=True)


def create_metabase_token():
    env_config = dotenv_values(".env")
    metabase_user = env_config.get('METABASE_USER')
    metabase_password = env_config.get('METABASE_PASSWORD')

    # Check if credentials are missing
    if not metabase_user or not metabase_password:
        raise Exception(f"Metabase credentials missing: USER='{metabase_user}', PASSWORD='{metabase_password}'")

    url = 'https://cubbo.metabaseapp.com/api/session'
    data = {'username': metabase_user, 'password': metabase_password}
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code == 200:
        return response.json().get('id')
    else:
        raise Exception(f'Failed to create token: {response.content}')
    
def get_dataset(question, params={}):
    METABASE_ENDPOINT = "https://cubbo.metabaseapp.com"
    METABASE_TOKEN = create_metabase_token()

    res = requests.post(METABASE_ENDPOINT + f'/api/card/{question}/query/json',
                        headers={"Content-Type": "application/json", 'X-Metabase-Session': METABASE_TOKEN},
                        params=params)
    print(res)
    return res.json()

def process_data(inputs):
    def create_param(tag, param_value):
        param = {'target': ["variable", ["template-tag", tag]]}
        if isinstance(param_value, int):
            param.update({'type': "number/=", 'value': param_value})
        elif isinstance(param_value, datetime):
            param.update({'type': "date/single", 'value': f"{param_value:%Y-%m-%d}"})
        else:
            param.update({'type': "category", 'value': param_value})
        return param

    return {'parameters': json.dumps([create_param(name, value) for name, value in inputs.items() if value is not None])}

class CONFIG:
    def __init__(self):
        self.JSON_CONFIG = self.get_configurations()
        self.FERIADOS_ACCOUNTS = self.JSON_CONFIG["BR"]["holidays"]

    def get_configurations(self):
        with open("json/config.json", "r") as f:
            return json.load(f)
        
config = CONFIG()

def save_to_redis(key, data):
    try:
        json_data = json.dumps(data)
        redis_client.set(key, json_data)
    except Exception as e:
        print(f"Error saving data to Redis: {e}")

def load_from_redis(key):
    try:
        json_data = redis_client.get(key)
        return json.loads(json_data) if json_data else {}
    except Exception as e:
        print(f"Error loading data from Redis: {e}")
        return {}

# Adjust functions like load_excluded_orders and save_excluded_orders to use Redis
def load_excluded_orders():
    return load_from_redis("excluded_orders") or []

def save_excluded_orders(excluded_orders):
    save_to_redis("excluded_orders", excluded_orders)

def load_excluded_recibos():
    return load_from_redis("excluded_recibos") or []

def save_excluded_recibos(excluded_recibos):
    save_to_redis("excluded_recibos", excluded_recibos)

def adjust_shipping_date(shipping_date, carrier):
    cut_off_hours = {
        'CUBBO': (16, 30),
        'UELLO': (16, 0),
        'CORREIOS': (13, 0),
        'LOGGI': (16, 0),
        'Mercado Envíos': (13, 0),
        'JT Express': (16, 0)
    }

    hour, minute = cut_off_hours.get(carrier, cut_off_hours['LOGGI'])

    # Check if shipping date is after cut off time or is a holiday
    if (shipping_date.hour > hour or 
        (shipping_date.hour == hour and shipping_date.minute >= minute) or 
        shipping_date.weekday() >= 5 or  # Check if it's Saturday or Sunday
        shipping_date.strftime('%Y-%m-%d') in config.JSON_CONFIG['BR']['holidays']):
        
        # Increment the shipping date by one day until it's not a holiday or weekend
        while True:
            shipping_date += timedelta(days=1)
            if (shipping_date.weekday() < 5 and 
                shipping_date.strftime('%Y-%m-%d') not in config.JSON_CONFIG['BR']['holidays']):
                break

    return shipping_date

def adjust_receiving_date(recibo):
    # Convert recibo to numpy datetime64[D] format
    recibo_np = np.datetime64(recibo.strftime('%Y-%m-%d'))

    adjusted_date_np = np.busday_offset(recibo_np, 1, roll='backward', holidays=config.JSON_CONFIG['BR']['holidays'])
    adjusted_datetime = datetime.utcfromtimestamp(adjusted_date_np.astype('datetime64[s]').astype(int))
    return adjusted_datetime

def last_workday_of_previous_month():
    today = datetime.now()
    first_day_of_this_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_this_month - timedelta(days=1)

    # Loop backwards from the last day of the month until we find a workday
    while last_day_of_previous_month.weekday() >= 5:  # Saturday or Sunday
        last_day_of_previous_month -= timedelta(days=1)
        last_day_of_previous_month = last_day_of_previous_month.replace(hour=5)
    
    # If the last day of the previous month is a holiday, go one day backward
    while last_day_of_previous_month.strftime("%Y-%m-%d") in config.JSON_CONFIG['BR']['holidays']:
        last_day_of_previous_month -= timedelta(days=1)

    print(last_day_of_previous_month)
    return last_day_of_previous_month


def ajuste_pendentes():
    
    excluded_orders = load_excluded_orders()

    sorted_data = []

    pending_at_start_date = last_workday_of_previous_month()

    incentivo_inputs = process_data(
        {
            'pending_at_start_date': pending_at_start_date,
            'wh': 4
        }
    )

    orders_list = get_dataset('1496', incentivo_inputs)

    #print(orders_list)

    marcas = ["FOSFORO", "Dois Pontos", "Boitempo", "Qura Editora", "TAG Livros"]


    for order in orders_list:
        #print(order)
        order_number = order['order_number']
        if order_number in excluded_orders:
            continue

        if order['status'] == "canceled" or order['status'] == "holded":
            continue
        
        order['pending_at'] = datetime.strptime(order['pending_at'], date_format)

        if order['pending_at'].date() in config.JSON_CONFIG['BR']['holidays']:
            order['pending_at'] += timedelta(days=1)
            
        if order['Stores__name'] in marcas:
            order['pending_at'] += timedelta(days=3)
            order['pending_at'] = order['pending_at'].replace(hour=9, minute=0, second=0)

        if order['Stores__name'] == "TAG Livros":
            continue

        order['pending_at'] = adjust_shipping_date(order['pending_at'],order['carrier_name'])

        if order['pending_at'].date() in config.JSON_CONFIG['BR']['holidays']:
            order['pending_at'] += timedelta(days=1)
 


        if order['shipping_date'] is not None and order['shipping_date'] !="":
            try:
                order['shipping_date'] = datetime.strptime(order['shipping_date'], date_format)
            except:
                order['shipping_date'] = datetime.strptime(order['shipping_date'], date_format2)
            if order['shipping_date'].month == datetime.now().month - 1:
                continue


            #if order['shipping_date'].day > 5 or order['pending_at'].day > 5:
                #continue


            carrier = order['carrier_name']
            if carrier == 'CORREIOS':
                shipping_time_limit = 15             
            elif carrier == 'Mercado Envíos':
                shipping_time_limit = 14  # Orders must be shipped by 14:30
            elif carrier == 'Armazém' or carrier == 'Externo' or carrier == 'LOGGI':
                shipping_time_limit = 17  # Orders must be shipped by 18:00
            else:
                shipping_time_limit = 18  # Default for all other cases, orders must be shipped by 19:30 
                # Check if the order meets the shipping time limit
            if order['shipping_date'].hour <= shipping_time_limit and order['pending_at'].day == order['shipping_date'].day and order['shipping_date'].month == order['pending_at'].month:
                order['SLA'] = "HIT"
            elif order['shipping_date'].day < order['pending_at'].day and order['shipping_date'].month == order['pending_at'].month:
                order['SLA'] = "HIT"
            elif order['shipping_date'].month < order['pending_at'].month:
                order['SLA'] = "HIT"
            else:
                order['SLA'] = "MISS"
        else:
            order['SLA'] = "MISS"
            order['shipping_date'] = "PROCESSANDO"

        if order['shipping_date'] == "PROCESSANDO" and order['pending_at'] > datetime.now():
            continue

        sorted_data.append({
            'order_number': order['order_number'],
            'store_name': order['Stores__name'],
            'tote_code': order['Totes__unique_code'],
            'carrier_name': order['carrier_name'],
            'pending_at': order['pending_at'],
            'Shipping Labels__created_at': order['shipping_date'],
            'SLA': order['SLA'],
            'picking_complete' : order['picking_complete']
            })


    return sorted_data

def incentivos_pedidos(todos_pedidos):

    excluded_orders = load_excluded_orders()
    hit_count = 0
    atraso = []

    for entry in todos_pedidos:
        # Check if the entry is a MISS and increment the counter
        if entry['SLA'] == "HIT":
            hit_count += 1
        if entry["SLA"] == "MISS":
            if entry['store_name'] != "ABOVE AVERAGE":
                atraso.append({
                    'tote_code': entry['tote_code'],
                    'order_number': entry['order_number'],
                    'store_name': entry['store_name'],
                    'pending_at': entry['pending_at'],
                    'Shipping Labels__created_at': entry['Shipping Labels__created_at'],
                    'carrier_name': entry['carrier_name']
                })
        

    for excluded_order in excluded_orders:
        # Remove the excluded orders from the atraso list
        atraso = [entry for entry in atraso if entry['order_number'] != excluded_order]

    sla_porcent = (hit_count / len(todos_pedidos)) * 100


    current_month = datetime.now().month

    pedidos_semana_1=1
    hit_semana_1=1

    for s1 in todos_pedidos:
        if s1['pending_at'].day<=8 or s1['pending_at'].month < current_month:
            pedidos_semana_1 +=1
            if s1['SLA']=="HIT":
                hit_semana_1+=1
        else:
            continue

    pedidos_semana_2=1
    hit_semana_2=1

    for s1 in todos_pedidos:
        if s1['pending_at'].day>8 and s1['pending_at'].day<=16:
            pedidos_semana_2 +=1
            if s1['SLA']=="HIT":
                hit_semana_2+=1
            #para imprimir pedidos com atraso da semana 2    
            #if s1['SLA']=="MISS":
                #print(s1)
        else:
            continue  

    pedidos_semana_3=1
    hit_semana_3=1
    
    for s1 in todos_pedidos:
        if s1['pending_at'].day>16 and s1['pending_at'].day<=24:
            pedidos_semana_3 +=1
            if s1['SLA']=="HIT":
                hit_semana_3+=1
            #para imprimir pedidos MISS semana 3
            #if s1['SLA']=="MISS" and s1['Shipping Labels__created_at'] != "PROCESSANDO" and s1['pending_at'].day == datetime.now():
                #print(s1)
        else:
            continue     

    pedidos_semana_4=1
    hit_semana_4=1

    for s1 in todos_pedidos:
        if s1['pending_at'].day>24 and s1['pending_at'].month == current_month:
            pedidos_semana_4 +=1
            if s1['SLA']=="HIT":
                hit_semana_4+=1
                    #para imprimir pedidos com atraso da semana 4    
            #if s1['SLA']=="MISS" and s1['Shipping Labels__created_at'] != "PROCESSANDO":
                #print(s1)
        else:
            continue     

    sla_semana_1 = "{:.2f}".format((hit_semana_1 / pedidos_semana_1) * 100)
    sla_semana_2 = "{:.2f}".format((hit_semana_2 / pedidos_semana_2) * 100)
    sla_semana_3 = "{:.2f}".format((hit_semana_3 / pedidos_semana_3) * 100)
    sla_semana_4 = "{:.2f}".format((hit_semana_4 / pedidos_semana_4) * 100)

    print("Todos os pedidos:")
    print(len(todos_pedidos))
    print("Pedidos em atraso")
    print(len(atraso))
    #print(atraso)

    return sla_semana_1, sla_semana_2, sla_semana_3, sla_semana_4, sla_porcent

def incentivos_recibo():

    excluded_recibos = load_excluded_recibos()
    recibos_data = []

    
    recibo_inputs = process_data(
        {
            'arrived_at': (datetime.now().replace(day=1) - timedelta(days=1)),
            'wh': 4
        }
    )

    recibos_list = get_dataset('1485', recibo_inputs)

    #print(excluded_recibos)
    for recibo in recibos_list:
        recibo_number = recibo['id']
        recibo_number = str(recibo_number)
        if recibo_number in excluded_recibos:
            continue


        if recibo['arrived_at'] is not None or recibo['arrived_at'] != "":
            recibo['arrived_at'] = datetime.strptime(recibo['arrived_at'], date_format)
            recibo_time = recibo['arrived_at'].time()
            recibo['arrived_at'] = adjust_receiving_date(recibo['arrived_at'])
            recibo['arrived_at'] = recibo['arrived_at'].replace(hour=recibo_time.hour, minute=recibo_time.minute, second=recibo_time.second)
        else:
            continue


        if recibo['completed_at'] is not None and recibo['completed_at'] != "":
            try:
                recibo['completed_at'] = datetime.strptime(recibo['completed_at'], date_format)
            except:
                recibo['completed_at'] = datetime.strptime(recibo['completed_at'], date_format2)

            if recibo['completed_at'].month == datetime.now().month - 1:
                continue
            if recibo['arrived_at'] > recibo['completed_at']:
                recibo['SLA']="HIT"          
            else:
                recibo['SLA']="MISS"
        else:
            recibo['completed_at'] = "PROCESSANDO"
            continue

        recibos_data.append({
            'id': recibo['id'],
            'status': recibo['status'],
            'Stores__name': recibo['Stores__name'],
            'arrived_at': recibo['arrived_at'],
            'completed_at': recibo['completed_at'],
            'dock_to_stock_in_days': recibo['dock_to_stock_in_days'],
            'SLA': recibo['SLA']
            })

    hit_count_recibos = 0
    atraso_recibo = []

    for check in recibos_data:
        # Check if the entry is a MISS and increment the counter
        if check['SLA'] == "HIT":
            hit_count_recibos += 1
        elif check["SLA"] == "MISS":
            atraso_recibo.append({
            'id': check['id'],
            'Stores__name' : check['Stores__name'],
            'status': check['status'],
            'arrived_at': check['arrived_at'],
            'completed_at': check['completed_at'],
            'dock_to_stock_in_days': check['dock_to_stock_in_days'],
            'SLA' : check['SLA']
            })
        else:
            continue


    for excluded_recibo in excluded_recibos:
        # Remove the excluded orders from the atraso list
        atraso_recibo = [r for r in atraso_recibo if r['id'] != excluded_recibo]

    todos_recibos=len(recibos_data)
    if todos_recibos == 0:
        todos_recibos = 1
    total_recibos = "Total Recibos: " +str(todos_recibos)
    SLA_recibos_total = (hit_count_recibos/todos_recibos) * 100
    SLA_recibos_format = round(SLA_recibos_total, 2)
    porcentagem_total_recibos="SLA Percentage recibos: "+ str(SLA_recibos_format)
    total_recibos_atrasos="Recibos MISS: " +str(len(atraso_recibo))

    #print(atraso_recibo)
    current_month = datetime.now().month

    recibos_semana_1=1
    hit_recibos_semana_1=1

    for r1 in recibos_data:
        if r1['arrived_at'].day<=8 or r1['arrived_at'].month < current_month:
            recibos_semana_1 +=1
            if r1['SLA']=="HIT":
                hit_recibos_semana_1+=1
        else:
            continue

    recibos_semana_2=1
    hit_recibos_semana_2=1


    for r2 in recibos_data:
        if r2['arrived_at'].day>8 and r2['arrived_at'].day<=16:
            recibos_semana_2 +=1
            if r2['SLA']=="HIT":
                hit_recibos_semana_2+=1
        else:
            continue  

    recibos_semana_3=1
    hit_recibos_semana_3=1
    
    for r3 in recibos_data:
        if r3['arrived_at'].day>16 and r3['arrived_at'].day<=24:
            recibos_semana_3 +=1
            if r3['SLA']=="HIT":
                hit_recibos_semana_3+=1
            #if r3['SLA']=="MISS":
                #print(r3)
        else:
            continue     

    recibos_semana_4=1
    hit_recibos_semana_4=1

    for r4 in recibos_data:
        if r4['arrived_at'].day>24 and r4['arrived_at'].month == current_month:
            recibos_semana_4 +=1
            if r4['SLA']=="HIT":
                hit_recibos_semana_4+=1
        else:
            continue     

    sla_recibo_1 = "{:.2f}".format((hit_recibos_semana_1 / recibos_semana_1) * 100)
    sla_recibo_2 = "{:.2f}".format((hit_recibos_semana_2 / recibos_semana_2) * 100)
    sla_recibo_3 = "{:.2f}".format((hit_recibos_semana_3 / recibos_semana_3) * 100)
    sla_recibo_4 = "{:.2f}".format((hit_recibos_semana_4 / recibos_semana_4) * 100)

    
    return sla_recibo_1, sla_recibo_2, sla_recibo_3, sla_recibo_4, SLA_recibos_total

    #for h in atraso_recibo:
        #print(h)

def incentivos_picking(picking_list_mes):

    pedidos_semana_1 = 1
    pedidos_semana_2 = 1
    pedidos_semana_3 = 1
    pedidos_semana_4 = 1
    pedidos_hj = 1

    pickings_semana_1 = 1
    pickings_semana_2 = 1
    pickings_semana_3 = 1
    pickings_semana_4 = 1
    pickings_hj = 1

    current_month = datetime.now().month


    for pedidos in picking_list_mes:

        if pedidos['pending_at'].day <= 8 or pedidos['pending_at'].month < current_month:
            pedidos_semana_1 += 1
        elif pedidos['pending_at'].day <= 16 and pedidos['pending_at'].day > 8 and pedidos['pending_at'].month == current_month:
            pedidos_semana_2 += 1
        elif pedidos['pending_at'].day <= 24 and pedidos['pending_at'].day > 16 and pedidos['pending_at'].month == current_month:
            pedidos_semana_3 += 1
        else:
            pedidos_semana_4 += 1

        if pedidos['pending_at'].day == datetime.now().day:
            pedidos_hj += 1


        if pedidos['picking_complete'] is not None and pedidos['picking_complete'] != "":
            try:
                pedidos['picking_complete'] = datetime.strptime(pedidos['picking_complete'], date_format)
            except:
                pedidos['picking_complete'] = datetime.strptime(pedidos['picking_complete'], date_format2)  
        else:
            continue

        if pedidos['picking_complete'].day <= pedidos['pending_at'].day and pedidos['pending_at'].day <= 8 or pedidos['pending_at'].month < current_month and pedidos['picking_complete'] is not None:
            pickings_semana_1 += 1
        elif pedidos['picking_complete'].day <= pedidos['pending_at'].day and pedidos['pending_at'].day <= 16 and pedidos['pending_at'].day > 8 and pedidos['pending_at'].month == current_month:
            pickings_semana_2 += 1
        elif pedidos['picking_complete'].day <= pedidos['pending_at'].day and pedidos['pending_at'].day <= 24 and pedidos['pending_at'].day > 16 and pedidos['pending_at'].month == current_month:
            pickings_semana_3 += 1
        elif pedidos['picking_complete'].day <= pedidos['pending_at'].day and pedidos['pending_at'].day > 24:
            pickings_semana_4 += 1
        else:
            continue
            
        if pedidos['picking_complete'].day <= pedidos['pending_at'].day and pedidos['pending_at'].day == datetime.now().day:
            pickings_hj += 1

            
    # comparar packing date com picking date e adicionar na semana!


    sla_picking_semana_1 = "{:.2f}".format((pickings_semana_1 / pedidos_semana_1) * 100)
    sla_picking_semana_2 = "{:.2f}".format((pickings_semana_2 / pedidos_semana_2) * 100)
    sla_picking_semana_3 = "{:.2f}".format((pickings_semana_3 / pedidos_semana_3) * 100)
    sla_picking_semana_4 = "{:.2f}".format((pickings_semana_4 / pedidos_semana_4) * 100)
    sla_picking_hoje = "{:.2f}".format((pickings_hj / pedidos_hj) * 100)

    if datetime.now().day <=8:
        sla_picking_total = float(sla_picking_semana_1)
    elif datetime.now().day > 8 and datetime.now().day <= 16:
        sla_picking_total = (float(sla_picking_semana_1) + float(sla_picking_semana_2)) / 2
    elif datetime.now().day > 16 and datetime.now().day <= 24:
        sla_picking_total = (float(sla_picking_semana_1) + float(sla_picking_semana_2) + float(sla_picking_semana_3)) / 3
    else:
        sla_picking_total = (float(sla_picking_semana_1) + float(sla_picking_semana_2) + float(sla_picking_semana_3) + float(sla_picking_semana_4)) / 4


    return sla_picking_semana_1, sla_picking_semana_2, sla_picking_semana_3, sla_picking_semana_4, sla_picking_total

def calculate_averages(sla_week1, sla_week2, sla_week3, sla_week4, sla_recibo_1, sla_recibo_2, sla_recibo_3, sla_recibo_4, sla_picking_semana_1, sla_picking_semana_2, sla_picking_semana_3, sla_picking_semana_4):
    s1_total = (float(sla_week1) + float(sla_recibo_1) + float(sla_picking_semana_1)) / 3
    s2_total = (float(sla_week2) + float(sla_recibo_2) + float(sla_picking_semana_2)) / 3
    s3_total = (float(sla_week3) + float(sla_recibo_3) + float(sla_picking_semana_3)) / 3
    s4_total = (float(sla_week4) + float(sla_recibo_4) + float(sla_picking_semana_4)) / 3
    return s1_total, s2_total, s3_total, s4_total

def ajuste_sla(ajuste):
    ajuste += ajuste
    return ajuste

def calculate_complementary(value):
    # Convert value to integer
    value_int = int(value)
    
    # Calculate complementary value
    complementary = 100 - value_int
    
    # Format result as string "<value> <complementary>"
    result = f"{value_int} {complementary}"

    result = str(result)
    
    return result
 

def main():
    # Make sure Redis operations have correct keys
    todos_pedidos = ajuste_pendentes()

    sla_semana_1, sla_semana_2, sla_semana_3, sla_semana_4, sla_porcent = incentivos_pedidos(todos_pedidos)
    sla_recibo_1, sla_recibo_2, sla_recibo_3, sla_recibo_4, SLA_recibos_total = incentivos_recibo()
    sla_picking_semana_1, sla_picking_semana_2, sla_picking_semana_3, sla_picking_semana_4, sla_picking_total = incentivos_picking(todos_pedidos)

    s1_total, s2_total, s3_total, s4_total = calculate_averages(
        sla_semana_1, sla_semana_2, sla_semana_3, sla_semana_4,
        sla_recibo_1, sla_recibo_2, sla_recibo_3, sla_recibo_4,
        sla_picking_semana_1, sla_picking_semana_2, sla_picking_semana_3, sla_picking_semana_4
    )

    # Calculate monthly SLA
    day_now = datetime.now().day
    if day_now <= 8:
        sla_mes = s1_total
    elif day_now <= 16:
        sla_mes = (s1_total + s2_total) / 2
    elif day_now <= 24:
        sla_mes = (s1_total + s2_total + s3_total) / 3
    else:
        sla_mes = (s1_total + s2_total + s3_total + s4_total) / 4

    ajuste_recibos, ajuste_picking, ajuste_pedidos = 0, 0, 0
    data_erros = load_from_redis("sla_data")

    if data_erros:
        ajuste_recibos = int(data_erros.get("ajuste_recibos", 0))
        ajuste_picking = int(data_erros.get("ajuste_picking", 0))
        ajuste_pedidos = int(data_erros.get("ajuste_pedidos", 0))

    SLA_recibos_total -= ajuste_recibos * 0.01
    sla_picking_total -= ajuste_picking * 0.01
    sla_porcent -= ajuste_pedidos * 0.01

    total_s1_circulo = calculate_complementary(s1_total)
    total_s2_circulo = calculate_complementary(s2_total)
    total_s3_circulo = calculate_complementary(s3_total)
    total_s4_circulo = calculate_complementary(s4_total)
    sla_total_circulo = calculate_complementary(sla_mes)
    sla_total_ci_circulo = calculate_complementary(SLA_recibos_total)
    sla_total_pi_circulo = calculate_complementary(sla_picking_total)
    sla_total_pa_circulo = calculate_complementary(sla_porcent)

    data = {
        "sla_semana_1": sla_semana_1,
        "sla_semana_2": sla_semana_2,
        "sla_semana_3": sla_semana_3,
        "sla_semana_4": sla_semana_4,
        "sla_porcent": sla_porcent,
        "sla_recibo_1": sla_recibo_1,
        "sla_recibo_2": sla_recibo_2,
        "sla_recibo_3": sla_recibo_3,
        "sla_recibo_4": sla_recibo_4,
        "sla_recibos_total": SLA_recibos_total,
        "sla_picking_semana_1": sla_picking_semana_1,
        "sla_picking_semana_2": sla_picking_semana_2,
        "sla_picking_semana_3": sla_picking_semana_3,
        "sla_picking_semana_4": sla_picking_semana_4,
        "sla_picking_total": sla_picking_total,
        "s1_total": s1_total,
        "s2_total": s2_total,
        "s3_total": s3_total,
        "s4_total": s4_total,
        "sla_mes": sla_mes
    }

    for key, value in data.items():
        try:
            float_value = float(value)
            data[key] = "{:.1f}".format(float_value) if float_value < 100 else "100."
        except ValueError:
            pass

    data.update({
        "total_s1_circulo": total_s1_circulo,
        "total_s2_circulo": total_s2_circulo,
        "total_s3_circulo": total_s3_circulo,
        "total_s4_circulo": total_s4_circulo,
        "sla_total_circulo": sla_total_circulo,
        "sla_total_ci_circulo": sla_total_ci_circulo,
        "sla_total_pi_circulo": sla_total_pi_circulo,
        "sla_total_pa_circulo": sla_total_pa_circulo,
        "ajuste_recibos": ajuste_recibos,
        "ajuste_picking": ajuste_picking,
        "ajuste_pedidos": ajuste_pedidos,
        "hora_agora": nova_hora
    })

    save_to_redis("sla_data", data)
    print("Done")
    print(datetime.now())

if __name__ == "__main__":
    main()