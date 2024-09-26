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

        sorted_data.append({
            'order_number': order['order_number'],
            'shipping_date': order['shipping_date'],
        })

    return sorted_data


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

    for order in sorted_data:
        shipping_date = order['shipping_date']
        if shipping_date is not None and shipping_date != "":
            if isinstance(shipping_date, datetime):
                date_str = shipping_date.strftime('%d-%m-%Y')  # Full date
                if date_str in orders_per_day:
                    orders_per_day[date_str] += 1
                else:
                    orders_per_day[date_str] = 1

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

    # For each day in the month, check if it's a weekday and not in phd_per_day
    for day in range(1, num_days_in_month + 1):
        date_obj = datetime(current_year, current_month, day)
        if date_obj.weekday() < 5:  # Monday=0, Sunday=6
            day_str = date_obj.strftime('%d')
            if day_str not in phd_per_day:
                phd_per_day[day_str] = 100

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

    # Write the sorted PHD per day to a JSON file
    with open('json/phd_per_day.json', 'w') as json_file:
        json.dump(sorted_phd_per_day, json_file, indent=4)

    # Optionally, return the sorted_phd_per_day dictionary
    return sorted_phd_per_day


if __name__ == "__main__":
    phd_json = compute_phd()
    print(phd_json)
