from metabase import get_dataset
from slack_bot_interface import send_message

def repo_prod_pp():
    prods_list = get_dataset('5523')

    message = []
    
    for prod in prods_list:
        prod['SKU'] = prod['SKU'].split('-')[0]
        lote_split = prod['SKU'].split(' ')[0]
        message.append(f"BIN: {prod['Warehouse Locations__unique_code']} || NOME: {prod['Products__name']} || SKU: {prod['SKU']} || EAN: {prod['Products__bar_code']} || LOTE: {lote_split} || QNT: {prod['count']}")
    message.sort()
    return message


if __name__ == "__main__":
    msg_prod_pp = repo_prod_pp()
    send_message(msg_prod_pp, "repo-zpp")