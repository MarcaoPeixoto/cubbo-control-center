from metabase import get_dataset
from slack_bot_interface import send_message
from datetime import datetime, timedelta
import os
from dotenv import dotenv_values

current_time = datetime.now()
ten_mins_ago = current_time - timedelta(minutes=10)

env_config = dotenv_values(".env")

date_format = os.environ["DATE_FORMAT"] or env_config.get('DATE_FORMAT')

date_format2 = os.environ["DATE_FORMAT2"] or env_config.get('DATE_FORMAT2')


def controle_fluxo_pedidos_natura():
    orders_list = get_dataset('8724')    
    message = ['lucas xereta']  # Initialize message as empty list

    print(orders_list)

    # Convert all dates first
    for order in orders_list:
        
        recent_orders = []

    for order in orders_list:
        try:
            order['minute'] = datetime.strptime(order['minute'], date_format)
        except ValueError:
            order['minute'] = datetime.strptime(order['minute'], date_format2)
    # Now process the orders
        if order['minute'] >= ten_mins_ago:
            if order['pedidos'] is not None:  # Add null check
                recent_orders.append(order['pedidos'])
    print(recent_orders)
    
    if recent_orders:
        #avg = sum(recent_orders) / len(recent_orders)
        avg = 1
        if avg < 5:
            message = ["Fluxo de pedidos abaixo de 5 por minuto!"]  # Replace default message with alert
    
    return message


if __name__ == "__main__":
    message = controle_fluxo_pedidos_natura()
    send_message(message, "teste-bot-marco")