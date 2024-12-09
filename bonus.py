import requests
import json
from datetime import datetime, timedelta
import time
import redis
from dotenv import dotenv_values
import os
import calendar
from redis_connection import get_redis_connection
from metabase import get_dataset, process_data
from parseDT import parse_date

env_config = dotenv_values(".env")

# Replace the existing redis_client creation with:
redis_client = get_redis_connection()


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
            order['shipping_date'] = parse_date(order['shipping_date'])

            if order['shipping_date'].month != today.month:
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
    #testes
    #phd_full = env_config.get('PHD_FULL')
    #envios_mes = env_config.get('ENVIOS_MES')
    #phd_full = float(phd_full)
    #envios_mes = int(envios_mes)



    nivel_de_bonus = "Nivel 0"
    multiplicador_bonus_sla = 0
    porcentagem_da_barra_sla = 0
    porcentagem_da_barra_pdh_mini = 0
    porcentagem_da_barra_pdh_full = 0
    sla_embu_full = json.load(open('json/sla_embu.json'))['sla_mes']
    sla_embu_full = float(sla_embu_full)

    #testes
    #sla_embu_full = env_config.get('SLA_EMBU_FULL')
    #sla_embu_full = float(sla_embu_full)

    phd_int = int(phd_full)
    phd_decimal = (phd_full - phd_int)*100

    if phd_int < 90:
        phd_int = 90

    if phd_full > 90:
        phd_mini_low = phd_int
        phd_mini_high = phd_int + 1
    if phd_full <= 90:
        phd_mini_low = 0
        phd_mini_high = 90

    valor_bonus = (phd_full - 90) * 0.01

    if valor_bonus < 0.01:
        valor_bonus = 0.01

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

    # Add the phd_bonus_dict to the sorted_phd_per_day dictionary
    if sla_embu_full < 90 or phd_full < 90:
        bonus_valido = False
        phd_full = 90
        sla_embu_full = 90
    else:
        bonus_valido = True
    
    if sla_embu_full >= 90 and sla_embu_full < 95:
        multiplicador_bonus_sla = 1
    elif sla_embu_full >= 95 and sla_embu_full < 97:
        multiplicador_bonus_sla = 1.2
    elif sla_embu_full >= 97 and sla_embu_full < 99:
        multiplicador_bonus_sla = 1.5
    elif sla_embu_full >= 99 and sla_embu_full <= 100:
        multiplicador_bonus_sla = 2
    

    for i in range(11):
        phd_var += 1
        valor_bonus_var = (phd_var - 90) * 0.01
        valor_barra_var = valor_bonus_var * envios_mes * multiplicador_bonus_sla
        valor_barra_var = round(valor_barra_var, 2)
        phd_bonus_dict[str(phd_var)] = valor_barra_var
        # print(f"PHD: {phd_var} --> Valor Barra: {valor_barra_var}")  # Commented out print statement

    # Calculate SLA bonus percentage
    porcentagem_da_barra_sla = (sla_embu_full - 90) * 10  
    valor_bonus_contagem = envios_mes * multiplicador_bonus_sla * valor_bonus
    valor_bonus_contagem = round(valor_bonus_contagem, 2)
    # Add additional variables to the JSON
    #colocar aqui as questões de bonificação para serem adicionadas ao json

    # Add additional variables to the JSON
    #colocar aqui as questões de bonificação para serem adicionadas ao json
    if phd_full > 90:
        porcentagem_da_barra_pdh_mini = round(phd_decimal, 2)
        porcentagem_da_barra_pdh_full = phd_combined*10
    if phd_full <= 90:
        porcentagem_da_barra_pdh_mini = avg_phd*0.9
        porcentagem_da_barra_pdh_full = 0

    sorted_phd_per_day['phd_full'] = phd_full   
    sorted_phd_per_day['sla_embu_full'] = sla_embu_full
    sorted_phd_per_day['bonus_valido'] = bonus_valido
    sorted_phd_per_day['valor_bonus_contagem'] = valor_bonus_contagem
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