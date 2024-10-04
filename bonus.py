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

    orders_list = get_dataset('3379', incentivo_inputs)

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
            if order['account_type'] != "CUSTOMER_ACCOUNT":
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
    today = datetime.today()

    for order in sorted_data:
        if order['shipping_date'] != "processando":
            shipping_date = order['shipping_date']
            if shipping_date is not None and shipping_date != "":
                envios_mes += 1
                if isinstance(shipping_date, datetime):
                    date_str = shipping_date.strftime('%d-%m-%Y')  # Full date
                    if date_str in orders_per_day:
                        orders_per_day[date_str] += 1
                    else:
                        orders_per_day[date_str] = 1
                    
                    if shipping_date.date() == today.date():
                        envios_hoje += 1
        else:
            pedidos_pendentes += 1
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


    if phd_per_day:
        avg_phd = sum(phd_per_day.values()) / len(phd_per_day)
        phd_full = avg_phd
        avg_phd = round(avg_phd, 2)
    else:
        avg_phd = None  # Or 0

    # Sort the phd_per_day dictionary by day
    sorted_phd_per_day = dict(sorted(phd_per_day.items(), key=lambda x: int(x[0])))

    # Add the average to the sorted dictionary



    nivel_de_bonus = "Nivel 0"
    multiplicador_bonus_sla = 0
    porcentagem_da_barra_sla = 0
    porcentagem_da_barra_pdh_mini = 0
    porcentagem_da_barra_pdh_full = 0
    sla_embu_full = json.load(open('json/sla_embu.json'))['sla_mes']
    sla_embu_full = float(sla_embu_full)


    phd_int = int(phd_full)
    phd_decimal = (phd_full - phd_int)*100

    if phd_int < 90:
        phd_int = 90

    phd_mini_low = phd_int
    phd_mini_high = phd_int + 1
    valor_bonus = (phd_full - 90) * 0.01

    if sla_embu_full < 95 or phd_full < 90:
        bonus_valido = False
        phd_full = 90
        sla_embu_full = 95
    else:
        bonus_valido = True

    phd_full_str = f"{phd_full}"
    phd_parts = phd_full_str.split('.')
    phd_first_digit = phd_parts[0][-1]
    phd_decimals = phd_parts[1] if len(phd_parts) > 1 else '0'    
    phd_combined = float(f"{phd_first_digit}.{phd_decimals}")


    phd_full_start = phd_full - phd_combined

    if phd_full_start < 90:
        phd_full_start = 90

    phd_var = phd_full_start - 1
    phd_bonus_dict = {}

    for i in range(11):
        phd_var += 1
        valor_bonus_var = (phd_var - 90) * 0.01
        valor_barra_var = valor_bonus_var * envios_mes
        valor_barra_var = round(valor_barra_var, 2)
        phd_bonus_dict[str(phd_var)] = valor_barra_var
        # print(f"PHD: {phd_var} --> Valor Barra: {valor_barra_var}")  # Commented out print statement

    # Add the phd_bonus_dict to the sorted_phd_per_day dictionary
    if sla_embu_full < 95 or phd_full < 90:
        bonus_valido = False
        phd_full = 90
        sla_embu_full = 95
    else:
        bonus_valido = True
    
    if sla_embu_full < 96:
        multiplicador_bonus_sla = 0.5
    elif sla_embu_full >= 96 and sla_embu_full < 97:
        multiplicador_bonus_sla = 0.75
    elif sla_embu_full >= 97 and sla_embu_full < 98:
        multiplicador_bonus_sla = 1
    elif sla_embu_full >= 98 and sla_embu_full < 99:
        multiplicador_bonus_sla = 1.25
    elif sla_embu_full >= 99 and sla_embu_full < 100:
        multiplicador_bonus_sla = 1.5
    
    # Calculate SLA bonus percentage
  # Linear interpolation between 95% and 100%
    porcentagem_da_barra_sla = (sla_embu_full - 95) * 20  # 20 is the factor to scale 0-5 to 0-100

    # Add additional variables to the JSON
    #colocar aqui as questões de bonificação para serem adicionadas ao json
    sorted_phd_per_day['bonus_valido'] = bonus_valido
    sorted_phd_per_day['phd_bonus_values'] = phd_bonus_dict
    sorted_phd_per_day['phd_mini_low'] = phd_mini_low
    sorted_phd_per_day['phd_mini_high'] = phd_mini_high
    sorted_phd_per_day['phd_decimal'] = phd_decimal
    sorted_phd_per_day['media'] = avg_phd
    sorted_phd_per_day['media_full'] = phd_full
    sorted_phd_per_day['multiplicador_bonus_sla'] = multiplicador_bonus_sla
    sorted_phd_per_day['porcentagem_da_barra_sla'] = porcentagem_da_barra_sla
    sorted_phd_per_day['porcentagem_da_barra_pdh_mini'] = porcentagem_da_barra_pdh_mini
    sorted_phd_per_day['porcentagem_da_barra_pdh_full'] = porcentagem_da_barra_pdh_full
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
