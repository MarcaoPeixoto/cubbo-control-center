import requests
import json
from datetime import datetime, timedelta
import time
import redis
from dotenv import dotenv_values
import os
import calendar

date_format = "%d-%m-%Y, %H:%M:%S"
date_format2 = "%d-%m-%Y, %H:%M:%S.%f"

env_config = dotenv_values(".env")

redis_end = env_config.get('REDIS_END')

if redis_end is not None:
    redis_port = env_config.get('REDIS_PORT')
    redis_password = env_config.get('REDIS_PASSWORD')
else:
    redis_end = os.environ["REDIS_END"]
    redis_port = os.environ["REDIS_PORT"]
    redis_password = os.environ["REDIS_PASSWORD"]

redis_client = redis.StrictRedis(host=redis_end, port=redis_port, password=redis_password, db=0, decode_responses=True)


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
                        params=params,
                        )
    dataset = res.json()
    return dataset


def process_data(inputs):
    def create_param(tag, param_value):
        param = {}
        if isinstance(param_value, int):
            param['type'] = "number/="
            param['value'] = param_value
        elif isinstance(param_value, datetime):
            param['type'] = "date/single"
            param['value'] = param_value.strftime("%Y-%m-%d")
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


dir_path = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(dir_path, "json/config.json"), "r") as f:
    CONFIG = json.load(f)

today = datetime.today()

# Extract current year and month
year = today.year
month = today.month

# Subtract two months
month -= 2

# Adjust year and month if month is less than or equal to zero
if month <= 0:
    month += 12
    year -= 1

# Find the last day of the target month
last_day = calendar.monthrange(year, month)[1]

# Create a date object for the last day
last_day_date = datetime(year=year, month=month, day=last_day)

def dt_processado():
    sorted_data = []


    incentivo_inputs = process_data(
        {
            'pending_at_start_date': last_day_date,
            'wh': 4
        }
    )

    orders_list = get_dataset('1496', incentivo_inputs)

    for order in orders_list:
        if order['shipping_date'] is not None and order['shipping_date'] != "":
            try:
                order['shipping_date'] = datetime.strptime(order['shipping_date'], date_format)
            except:
                order['shipping_date'] = datetime.strptime(order['shipping_date'], date_format2)

            shipping_date = order['shipping_date']

            if shipping_date.month != today.month:
                continue
        else:
            order['shipping_date'] = "processando"

        sorted_data.append({
            'order_number': order['order_number'],
            'shipping_date': order['shipping_date'],
        })

    return sorted_data

operators_month_ideal = 30
operators_month_real = 25

phd_ideal = 100
phd_real = 90



def compute_phd():
    # Load the operators per day data from Redis
    operators_per_day_json = redis_client.get('phd_operators')
    if operators_per_day_json is not None:
        operators_per_day = json.loads(operators_per_day_json)
    else:
        # If not found in Redis, handle the error or assign an empty dictionary
        print("No operator data found in Redis. Using empty operators_per_day.")
        operators_per_day = {}

    sorted_data = dt_processado()

    # Initialize orders per day dictionary
    orders_per_day = {}

    pedidos_pendentes = 0
    envios_hoje = 0
    envios_mes = 0

    for order in sorted_data:
        if order['shipping_date'] != "processando":
            envios_mes += 1
            shipping_date = order['shipping_date']
            if shipping_date is not None and shipping_date != "":
                if isinstance(shipping_date, datetime):
                    date_str = shipping_date.strftime('%d-%m-%Y')  # Full date
                    if date_str in orders_per_day:
                        orders_per_day[date_str] += 1
                    else:
                        orders_per_day[date_str] = 1
        else:
            pedidos_pendentes += 1

    today = datetime.today()  # Define 'today' at the beginning of the function
    for order in sorted_data:
        shipping_date = order['shipping_date']
        if shipping_date != "processando":
            if isinstance(shipping_date, str):
                try:
                    shipping_date = datetime.strptime(shipping_date, date_format)
                except ValueError:
                    shipping_date = datetime.strptime(shipping_date, date_format2)
            
            if shipping_date.day == today.day:
                envios_hoje += 1


    # Compute PHD per day
    phd_per_day_full_date = {}

    for day, orders_shipped in orders_per_day.items():
        operators = operators_per_day.get(day)
        if operators:
            phd = orders_shipped / operators
            phd_per_day_full_date[day] = phd
        else:
            # Handle days where operator data is missing
            print(f"No operator data for {day}. Skipping PHD calculation for this day.")

    # Round PHD values to 2 decimal places and modify keys before saving to JSON
    phd_per_day = {}
    for full_date, phd_value in phd_per_day_full_date.items():
        day_only = full_date[:2]  # Extract day from 'DD-MM-YYYY'
        phd_per_day[day_only] = round(phd_value, 2)

    # Now add missing workdays of the month with value 100
    # Get current month and year
    today = datetime.today()
    current_month = today.month
    current_year = today.year

    # Get number of days in current month
    num_days_in_month = calendar.monthrange(current_year, current_month)[1]

    # Calculate the number of workdays in the current month
    workdays_in_month = sum(1 for day in range(1, num_days_in_month + 1)
                            if datetime(current_year, current_month, day).weekday() < 5)

    pedidos_ideal_mes = workdays_in_month * operators_month_ideal * phd_ideal
    pedidos_real_mes = workdays_in_month * operators_month_real * phd_real

    # For each day in the month, check if it's a weekday and not in phd_per_day
    for day in range(1, num_days_in_month + 1):
        date_obj = datetime(current_year, current_month, day)
        if date_obj.weekday() < 5:  # Monday=0, Sunday=6
            day_str = date_obj.strftime('%d')
            if day_str not in phd_per_day:
                phd_per_day[day_str] = phd_ideal
    # Sort the phd_per_day dictionary by day
    sorted_phd_per_day = dict(sorted(phd_per_day.items(), key=lambda x: int(x[0])))

    # Compute average PHD value including the added 100s
    if phd_per_day:
        avg_phd = sum(phd_per_day.values()) / len(phd_per_day)
        avg_phd = round(avg_phd, 2)
        sorted_phd_per_day['media'] = avg_phd
    else:
        avg_phd = None  # Or 0
        sorted_phd_per_day['media'] = avg_phd

    #colocar aqui as questões de bonificação
    #os ifs para calcular o nivel de bonus e o que vai aparecer
    #assim como o valor do bonus por pedido e o multiplicador de bonus

    nivel_de_bonus = "Nivel 0"
    valor_bonus = 0
    multiplicador_bonus = 0
    porcentagem_da_barra = 0

    if envios_mes <= 55000:
        porcentagem_da_barra = (envios_mes / 55000) * 100
    elif envios_mes <= 60000:
        porcentagem_da_barra = ((envios_mes - 55000) / 5000) * 100
    elif envios_mes <= 65000:
        porcentagem_da_barra = ((envios_mes - 60000) / 5000) * 100
    elif envios_mes <= 70000:
        porcentagem_da_barra = ((envios_mes - 65000) / 5000) * 100
    elif envios_mes <= 100000:
        porcentagem_da_barra = ((envios_mes - 70000) / 30000) * 100
    else:
        porcentagem_da_barra = 100

    porcentagem_da_barra = round(porcentagem_da_barra, 2)

    if envios_mes > 55000:
        nivel_de_bonus = "Nivel 1"
        valor_bonus = 0.01
    if envios_mes > 60000:
        nivel_de_bonus = "Nivel 2"
        valor_bonus = 0.12
    if envios_mes > 65000:
        nivel_de_bonus = "Nivel 3"
        valor_bonus = 0.15
    if envios_mes > 70000:
        nivel_de_bonus = "Nivel 4"
        valor_bonus = 0.2
    if envios_mes > 100000:
        nivel_de_bonus = "Nivel 5"
        valor_bonus = 0.25
    
    if avg_phd >= 100:
        avg_phd = 100
    else:
        avg_phd = avg_phd
    multiplicador_bonus = round(avg_phd*0.01, 2)
        

    
    valor_bonus_total = envios_mes * valor_bonus * multiplicador_bonus
    # Add additional variables to the JSON
    #colocar aqui as questões de bonificação para serem adicionadas ao json
    sorted_phd_per_day['valor_bonus_total'] = valor_bonus_total
    sorted_phd_per_day['porcentagem_da_barra'] = porcentagem_da_barra
    sorted_phd_per_day['multiplicador_bonus'] = multiplicador_bonus
    sorted_phd_per_day['nivel_de_bonus'] = nivel_de_bonus
    sorted_phd_per_day['valor_bonus'] = valor_bonus
    sorted_phd_per_day['pedidos_pendentes'] = pedidos_pendentes
    sorted_phd_per_day['envios_dia'] = envios_hoje
    sorted_phd_per_day['envios_mes'] = envios_mes

    # Write the sorted PHD per day to a JSON file
    with open('json/phd_per_day.json', 'w') as json_file:
        json.dump(sorted_phd_per_day, json_file, indent=4)
    # Optionally, return the sorted_phd_per_day dictionary
    return sorted_phd_per_day



if __name__ == "__main__":
    phd_json = compute_phd()
    print(phd_json)
