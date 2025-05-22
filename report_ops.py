import requests
import json
from datetime import datetime, timedelta
import time
import redis
import gspread
from google.oauth2.credentials import Credentials
from google_auth import authenticate_google
from redis_connection import get_redis_connection
# Import the metabase functions
from metabase import get_dataset, process_data

# Get the Redis client from the shared connection
redis_client = get_redis_connection()

# Get credentials from Redis (since that's where google_auth.py stores them)
token_json = redis_client.get('token_json')
if not token_json:
    raise Exception("No token found in Redis. Run authentication first.")

# Create credentials object from the token
creds = Credentials.from_authorized_user_info(json.loads(token_json))
gc = gspread.authorize(creds)

date_format = "%Y-%m-%dT%H:%M:%S"
date_format2 = "%Y-%m-%dT%H:%M:%S.%f"

redis_end = "redis-14593.c99.us-east-1-4.ec2.redns.redis-cloud.com"
redis_port = 14593
redis_password = "xKz5lQL3kdYmNUbPiyvuj8DNCNxwaruk"

redis_client = redis.StrictRedis(host=redis_end, port=redis_port, password=redis_password, db=0, decode_responses=True)

# Fetch data from Google Sheets
def get_google_sheet(sheet_url, sheet_name):
    sheet = gc.open_by_url(sheet_url).worksheet(sheet_name)
    data = sheet.get_all_records()
    return data

# We no longer need to define these functions since we're importing them from metabase.py
# def create_metabase_token(): ...
# def get_dataset(question, params={}): ...
# def process_data(inputs): ...

holidays = ["2024-01-01","2024-01-25","2024-02-12","2024-03-29","2024-04-21","2024-05-01",
        "2024-05-30","2024-07-09","2024-09-07","2024-10-12","2024-11-02","2024-11-15",
        "2024-11-20","2024-12-25","2024-12-31"]

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

    if (shipping_date.hour > hour or
        (shipping_date.hour == hour and shipping_date.minute >= minute) or
        shipping_date.weekday() >= 5 or
        shipping_date.strftime('%Y-%m-%d') in holidays):

        while True:
            shipping_date += timedelta(days=1)
            if (shipping_date.weekday() < 5 and
                shipping_date.strftime('%Y-%m-%d') not in holidays):
                break

    return shipping_date

print("1 feito")

def ajuste_pendentes(only_process_today=True, max_day_retries=5):
    sorted_data = []
    
    # For full historical processing
    if not only_process_today:
        # Start with a baseline date and set the end date to today
        start_date = datetime.strptime('21/1/2024', '%d/%m/%Y')
        end_date = datetime.now()
        
        # Create a list of dates to process
        current_date = start_date
        dates_to_process = []
        
        while current_date <= end_date:
            # Skip weekends and holidays for historical processing
            if current_date.weekday() < 5 and current_date.strftime('%Y-%m-%d') not in holidays:
                dates_to_process.append(current_date)
            current_date += timedelta(days=1)
        
        print(f"Processing {len(dates_to_process)} days of historical data")
    else:
        # Just process today's data - always process today regardless of weekends/holidays
        today = datetime.now()
        dates_to_process = [today]
        
        # Log if it's a weekend or holiday, but still process
        if today.weekday() >= 5:
            print(f"Note: Today ({today.strftime('%d/%m/%Y')}) is a weekend, but still processing data")
        elif today.strftime('%Y-%m-%d') in holidays:
            print(f"Note: Today ({today.strftime('%d/%m/%Y')}) is a holiday, but still processing data")
        else:
            print("Processing today's data")
    
    # Process each date individually
    for day_to_process in dates_to_process:
        day_data = process_single_day(day_to_process, max_retries=max_day_retries)
        if day_data:
            sorted_data.extend(day_data)
    
    print(f"Finished processing. Total records: {len(sorted_data)}")
    return sorted_data

def process_single_day(day_to_process, max_retries=5):
    """
    Process a single day with multiple retries if needed.
    Returns list of processed orders for the day.
    """
    day_data = []
    retry_count = 0
    
    print(f"Processing data for {day_to_process.strftime('%d/%m/%Y')}")
    
    while retry_count < max_retries:
        try:
            # Get data for just this one day
            next_day = day_to_process + timedelta(days=1)
            
            # Use the imported process_data function - metabase.py expects actual datetime objects
            incentivo_inputs = process_data({
                'pending_at_start_date': day_to_process,
                'pending_at_end_date': next_day,
                'wh': 4
            })
            
            # Increasing timeout for difficult queries
            daily_orders = get_dataset('6512', incentivo_inputs)
            
            # Check if we got valid data back
            if not daily_orders or not isinstance(daily_orders, list):
                retry_count += 1
                wait_time = retry_count * 5  # Increasing wait time with each retry
                print(f"Attempt {retry_count}/{max_retries}: No valid data returned, waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                continue
                
            print(f"Retrieved {len(daily_orders)} orders for {day_to_process.strftime('%d/%m/%Y')}")
            
            # Process the daily orders
            marcas = ["FOSFORO", "Dois Pontos", "Boitempo", "Qura Editora"]
            
            for order in daily_orders:
                # Ensure all required fields exist before processing
                required_fields = ['pending_at', 'carrier_name', 'Stores__name', 'order_number']
                if not all(field in order for field in required_fields):
                    missing = [field for field in required_fields if field not in order]
                    print(f"Order missing required fields: {missing}, skipping...")
                    continue
                
                # Process pending_at date
                try:
                    order['pending_at'] = datetime.strptime(order['pending_at'], date_format)
                except:
                    try:
                        order['pending_at'] = datetime.strptime(order['pending_at'], date_format2)
                    except Exception as e:
                        print(f"Could not parse pending_at date: {order['pending_at']}, error: {e}")
                        continue
                
                if order['pending_at'].strftime('%Y-%m-%d') in holidays:
                    order['pending_at'] += timedelta(days=1)
                
                order['pending_at'] = adjust_shipping_date(order['pending_at'], order['carrier_name'])
                
                if order['Stores__name'] in marcas:
                    order['pending_at'] += timedelta(days=2)
                    order['pending_at'] = order['pending_at'].replace(hour=9, minute=0, second=0)
                
                if order['Stores__name'] == "TAG Livros":
                    order['pending_at'] += timedelta(days=15)
                    order['pending_at'] = order['pending_at'].replace(hour=9, minute=0, second=0)
                
                if order['shipping_date'] is not None and order['shipping_date'] != "":
                    try:
                        order['shipping_date'] = datetime.strptime(order['shipping_date'], date_format)
                    except:
                        try:
                            order['shipping_date'] = datetime.strptime(order['shipping_date'], date_format2)
                        except Exception as e:
                            print(f"Could not parse shipping_date: {order['shipping_date']}, error: {e}")
                            order['shipping_date'] = "PROCESSANDO"
                            order['SLA'] = "MISS"
                            continue
                            
                    # Process carrier and SLA checks
                    carrier = order['carrier_name']
                    if carrier == 'CORREIOS':
                        shipping_time_limit = 15
                    elif carrier == 'Mercado Envíos':
                        shipping_time_limit = 14
                    elif carrier in ['Armazém', 'Externo', 'LOGGI']:
                        shipping_time_limit = 17
                    else:
                        shipping_time_limit = 18
                        
                    # Determine SLA status
                    try:
                        if order['shipping_date'].hour <= shipping_time_limit and order['pending_at'].day == order['shipping_date'].day and order['shipping_date'].month == order['pending_at'].month:
                            order['SLA'] = "HIT"
                        elif order['shipping_date'].day < order['pending_at'].day and order['shipping_date'].month == order['pending_at'].month:
                            order['SLA'] = "HIT"
                        elif order['shipping_date'].month < order['pending_at'].month:
                            order['SLA'] = "HIT"
                        else:
                            order['SLA'] = "MISS"
                    except Exception as e:
                        print(f"Error determining SLA status: {e}")
                        order['SLA'] = "MISS"
                else:
                    order['SLA'] = "MISS"
                    order['shipping_date'] = "PROCESSANDO"

                # Skip orders that are not yet due
                try:
                    if order['shipping_date'] == "PROCESSANDO" and order['pending_at'] > datetime.now():
                        continue
                except Exception as e:
                    print(f"Error comparing dates: {e}")
                    continue

                # Add to results
                try:
                    day_data.append({
                        'order_number': order['order_number'],
                        'store_name': order['Stores__name'],
                        'tote_code': order.get('Totes__unique_code', ''),
                        'carrier_name': order['carrier_name'],
                        'pending_at': order['pending_at'],
                        'shipping_date': order['shipping_date'],
                        'SLA': order['SLA'],
                        'picking_complete': order.get('picking_complete', '')
                    })
                except Exception as e:
                    print(f"Error adding order to results: {e}")
                    continue
            
            # If we get here, we succeeded - break out of retry loop
            break
            
        except Exception as e:
            retry_count += 1
            wait_time = retry_count * 5  # Increasing wait time with each retry
            print(f"Error processing {day_to_process.strftime('%d/%m/%Y')} (attempt {retry_count}/{max_retries}): {str(e)}")
            print(f"Waiting {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            
            # If we've exhausted all retries, log the failure but don't stop the script
            if retry_count >= max_retries:
                print(f"FAILED to process {day_to_process.strftime('%d/%m/%Y')} after {max_retries} attempts")
    
    return day_data

def generate_csv(data, google_sheet_data, sheet_url, only_process_today=True):
    # Get existing data if we're only processing today
    existing_data = []
    if only_process_today:
        try:
            sheet = gc.open_by_url(sheet_url)
            existing_sheet_name = "dados_colab"
            
            # Check if sheet exists
            worksheets = [ws.title for ws in sheet.worksheets()]
            if existing_sheet_name in worksheets:
                try:
                    worksheet = sheet.worksheet(existing_sheet_name)
                    existing_records = worksheet.get_all_records()
                    
                    print(f"Found {len(existing_records)} records in existing sheet")
                    
                    # Convert the existing records to our data format
                    for record in existing_records:
                        # Skip header row and empty rows
                        if not record or 'DATE' not in record:
                            continue
                            
                        try:
                            date_str = record['DATE']
                            
                            existing_data.append({
                                'date': date_str,
                                'HIT': int(record.get('HIT', 0)),
                                'MISS': int(record.get('MISS', 0)),
                                'TOTAL_SHIPPED': int(record.get('TOTAL SHIPPED', 0)),
                                'PENDING': int(record.get('PENDING', 0)),
                                'SLA': float(record.get('SLA (%)', 0)),
                                'SHIPMENTS_PER_OPERATOR': float(record.get('SHIPMENTS PER OPERATOR', 0))
                            })
                        except Exception as e:
                            print(f"Error parsing existing record {record}: {e}")
                            continue
                    
                    print(f"Successfully loaded {len(existing_data)} existing records")
                except Exception as e:
                    print(f"Error reading existing data: {e}")
            else:
                print(f"Sheet '{existing_sheet_name}' does not exist yet. Will create it.")
        except Exception as e:
            print(f"Error accessing existing data: {e}")
    
    # Process current data
    date_summary = {}
    operators_data = {entry['Date']: entry['Operators'] for entry in google_sheet_data}

    # Filter operators_data for entries in the current month
    current_month = datetime.now().month
    current_year = datetime.now().year
    current_month_operators_data = {date: operators for date, operators in operators_data.items() if datetime.strptime(date, '%d-%m-%Y').month == current_month and datetime.strptime(date, '%d-%m-%Y').year == current_year}

    # Save to Redis
    operators_redis = json.dumps(current_month_operators_data)
    redis_key = "phd_operators"
    redis_client.set(redis_key, operators_redis)
    print(f"Saved operators data for the current month to Redis under key '{redis_key}'.")
    
    # Initialize date_summary with existing data first - this is critical to preserve historical data
    if existing_data and only_process_today:
        today_str = datetime.now().strftime('%d-%m-%Y')
        
        for record in existing_data:
            try:
                date_str = record['date']
                
                # Skip today's date since we'll reprocess it
                if date_str == today_str:
                    print(f"Skipping existing data for today ({today_str}) as it will be reprocessed")
                    continue
                
                # Add historical data to our summary
                date_summary[date_str] = {
                    'HIT': record['HIT'],
                    'MISS': record['MISS'],
                    'TOTAL_SHIPPED': record['TOTAL_SHIPPED'],
                    'PENDING': record['PENDING'],
                    'TAG_MISS': 0  # We don't have this from historical data
                }
                
                print(f"Added historical data for {date_str}")
            except Exception as e:
                print(f"Error adding historical record {record}: {e}")
    
    # Now process today's data and add/update the corresponding entries
    for entry in data:
        try:
            # Process pending date
            pending_date_str = entry['pending_at'].strftime('%d-%m-%Y')
            
            if pending_date_str not in date_summary:
                date_summary[pending_date_str] = {
                    'HIT': 0, 'MISS': 0, 
                    'TOTAL_SHIPPED': 0,
                    'PENDING': 0,
                    'TAG_MISS': 0
                }
            
            # Count as pending
            date_summary[pending_date_str]['PENDING'] += 1
            
            # Skip shipping date processing if not shipped yet
            if entry['shipping_date'] == "PROCESSANDO":
                continue
            
            # Process shipping date data
            shipping_date_str = entry['shipping_date'].strftime('%d-%m-%Y')
            if shipping_date_str not in date_summary:
                date_summary[shipping_date_str] = {
                    'HIT': 0, 'MISS': 0, 
                    'TOTAL_SHIPPED': 0, 
                    'PENDING': 0,
                    'TAG_MISS': 0
                }
            
            # Count as shipped
            date_summary[shipping_date_str]['TOTAL_SHIPPED'] += 1
            
            # Update SLA counters
            if entry['SLA'] == 'HIT':
                date_summary[shipping_date_str]['HIT'] += 1
            elif entry['SLA'] == 'MISS':
                # Check if it's TAG Livros
                if entry['store_name'] == "TAG Livros":
                    date_summary[pending_date_str]['TAG_MISS'] += 1
                else:
                    date_summary[pending_date_str]['MISS'] += 1
        except Exception as e:
            print(f"Error processing entry for summary: {e}")
            continue

    # Sort dates chronologically
    sorted_dates = sorted(date_summary.keys(), key=lambda x: datetime.strptime(x, '%d-%m-%Y'))

    # Prepare sheet data
    new_sheet_data = []
    new_sheet_data.append(['DATE', 'HIT', 'MISS', 'TOTAL SHIPPED', 'PENDING', 'SLA (%)', 'SHIPMENTS PER OPERATOR'])
    
    for date in sorted_dates:
        try:
            summary = date_summary[date]
            
            # Skip dates with no shipments
            if summary['TOTAL_SHIPPED'] == 0:
                continue
                
            # Calculate SLA percentage (excluding TAG Livros misses)
            total_for_sla = summary['HIT'] + summary['MISS']
            sla_percentage = round((summary['HIT'] / total_for_sla * 100), 2) if total_for_sla > 0 else 0
            
            # Get operator count, defaulting to 1 if not available
            operators = operators_data.get(date, 1)
            if operators <= 0:
                operators = 1
                
            # Calculate shipments per operator
            shipments_per_operator = round(summary['TOTAL_SHIPPED'] / operators, 2)
            
            new_sheet_data.append([
                date, 
                summary['HIT'], 
                summary['MISS'], 
                summary['TOTAL_SHIPPED'], 
                summary['PENDING'],
                sla_percentage, 
                shipments_per_operator
            ])
        except Exception as e:
            print(f"Error processing date {date} for sheet data: {e}")
            continue

    # Write to sheet
    sheet = gc.open_by_url(sheet_url)
    new_sheet_name = "dados_colab"
    if new_sheet_name in [ws.title for ws in sheet.worksheets()]:
        worksheet = sheet.worksheet(new_sheet_name)
        sheet.del_worksheet(worksheet)
    worksheet = sheet.add_worksheet(title=new_sheet_name, rows="1000", cols="20")

    # Batch update
    cell_range = f'A1:{chr(64 + len(new_sheet_data[0]))}{len(new_sheet_data)}'
    cell_list = worksheet.range(cell_range)

    for i, value in enumerate(sum(new_sheet_data, [])):
        cell_list[i].value = value

    worksheet.update_cells(cell_list)

    print(f"Data has been written to the '{new_sheet_name}' tab in the Google Sheet.")

if __name__ == "__main__":
    # Default: only process today's data
    only_process_today = False
    
    # Process full history if an argument is passed
    # You can enable this through command line arguments if needed
    
    data = ajuste_pendentes(only_process_today)
    google_sheet_url = 'https://docs.google.com/spreadsheets/d/1mpFed0ZENWecHYT_VmHj6Kx_5JSBXC8Q4hrm7RU-4u0/edit#gid=395863523'
    google_sheet_data = get_google_sheet(google_sheet_url, 'Operadores/dia')
    generate_csv(data, google_sheet_data, google_sheet_url, only_process_today)