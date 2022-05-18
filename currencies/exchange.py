import constant as my_constant
from config.config import *
import requests
from logger import print_log


def get_btc_yfi_exchange():
    price = {'ex_price': 0.0, 'ex_reverse': 0.0}
    try:
        data = my_constant.COIN_GECKO_API.get_price(ids='bitcoin', vs_currencies='yfi')
        data = data['bitcoin']['yfi']
        result = float(data) * (1 - (EXCHANGE_FEE_DICT['YFI'] / 100))
        price['ex_price'] = result
        result = 1 / (float(data) * (1 + (EXCHANGE_FEE_DICT['YFI'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 100))
        price['ex_reverse'] = result
    except Exception as err:
        print_log("get_btc_yfi_exchange:" + str(err), "ERROR", 3)
    return price


def get_btc_usd_exchange():
    try:
        data = my_constant.COIN_GECKO_API.get_price(ids=['bitcoin'], vs_currencies='usd')
        bitcoin = float(data['bitcoin']['usd'])
        return bitcoin
    except Exception as err:
        print_log("get_btc_usd_exchange:" + str(err), "ERROR", 3)
    return my_constant.EX_BTC_USD


def get_btc_usdt_exchange():
    price = {'ex_price': 0.0, 'ex_reverse': 0.0}
    try:
        data = my_constant.COIN_GECKO_API.get_price(ids=['bitcoin', 'tether'], vs_currencies='usd')
        bitcoin = data['bitcoin']['usd']
        tether = data['tether']['usd']
        if tether == 0:
            return price
        exchange = float(bitcoin / tether)
        result = exchange * (1 - (EXCHANGE_FEE_DICT['USDT'] / 100))
        price['ex_price'] = result
        result = 1 / (exchange * (1 + (EXCHANGE_FEE_DICT['USDT'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 100))
        price['ex_reverse'] = result
    except Exception as err:
        print_log("get_btc_usdt_exchange:" + str(err), "ERROR", 3)
    return price


def get_btc_wbtc_exchange():
    price = {'ex_price': 0.0, 'ex_reverse': 0.0}
    try:
        data = my_constant.COIN_GECKO_API.get_price(ids=['bitcoin', 'wrapped-bitcoin'], vs_currencies='usd')
        bitcoin = data['bitcoin']['usd']
        wbitcoin = data['wrapped-bitcoin']['usd']
        if wbitcoin == 0:
            return price
        exchange = float(bitcoin / wbitcoin)
        result = exchange * (1 - (EXCHANGE_FEE_DICT['WBTC'] / 100))
        price['ex_price'] = result
        result = 1 / (exchange * (1 + (EXCHANGE_FEE_DICT['WBTC'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 100))
        price['ex_reverse'] = result
    except Exception as err:
        print_log("get_btc_wbtc_exchange:" + str(err), "ERROR", 3)
    return price

def get_btc_vey_exchange():
    price = {'ex_price': 0.0, 'ex_reverse': 0.0}
    try:
        price['ex_price'] = 70000
        price['ex_reverse'] = 1 / 70000
        return price
        html = requests.get(my_constant.VEY_ENDPOINT)
        if html.status_code > 300:
            return price
        html = html.content.decode()
        x = html.find('<span id=\"market_last_price_val\">')
        if x == -1:
            return price
        x += len('<span id=\"market_last_price_val\">')
        y = html.find('</span> <span class=\"h24currency2\">')
        ex_str = ''
        for i in range(x, y):
            ex_str += html[i]
        exchange = float(ex_str)
        if exchange == 0:
            return price
        result = 1 / (exchange * (1 + (EXCHANGE_FEE_DICT['VEY'] / 100)))
        price['ex_price'] = result
        result = exchange * (1 - ((EXCHANGE_FEE_DICT['VEY'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE)  / 100))
        price['ex_reverse'] = result
        return price
    except Exception as e:
        print_log("get_btc_vey_exchange:" + str(e), "ERROR", 3)
        return price

def get_btc_eth_exchange():
    price = {'ex_price': 0.0, 'ex_reverse': 0.0}
    try:
        data = my_constant.COIN_GECKO_API.get_price(ids='bitcoin', vs_currencies='eth')
        bitcoin = data['bitcoin']['eth']
        if bitcoin == 0:
            return price
        exchange = float(bitcoin)
        result = exchange * (1 - (EXCHANGE_FEE_DICT['ETH'] / 100))
        price['ex_price'] = result
        result = 1 / (exchange * (1 + (EXCHANGE_FEE_DICT['ETH'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 100))
        price['ex_reverse'] = result
    except Exception as err:
        print_log("get_btc_eth_exchange:" + str(err), "ERROR", 3)
    return price


def get_btc_xvg_exchange():
    try:
        price = {'ex_price': 0.0, 'ex_reverse': 0.0}
        price['ex_price'] = 10000
        price['ex_reverse'] = 1 / 10000
        return price
        data = my_constant.COIN_GECKO_API.get_price(ids=['bitcoin', 'verge'], vs_currencies='usd')
        bitcoin = data['bitcoin']['usd']
        verge = data['verge']['usd']
        if verge == 0:
            return None
        exchange = float(bitcoin / verge)
        result = exchange * (1 - (EXCHANGE_FEE_DICT['XVG'] / 100))
        price['ex_price'] = result
        result = 1 / (exchange * (1 + (EXCHANGE_FEE_DICT['XVG'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 100))
        price['ex_reverse'] = result
        return price
    except Exception as e:
        print_log("get_btc_xvg_exchange:" + str(e), "ERROR", 3)
        return None


def get_btc_xmr_exchange():
    try:
        price = {'ex_price': 0.0, 'ex_reverse': 0.0}
        data = my_constant.COIN_GECKO_API.get_price(ids=['bitcoin', 'monero'], vs_currencies='usd')
        bitcoin = data['bitcoin']['usd']
        monero = data['monero']['usd']
        if monero == 0:
            return None
        exchange = float(bitcoin / monero)
        result = exchange * (1 - (EXCHANGE_FEE_DICT['XMR'] / 100))
        price['ex_price'] = result
        result = 1 / (exchange * (1 + (EXCHANGE_FEE_DICT['XMR'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 100))
        price['ex_reverse'] = result
        return price
    except Exception as e:
        print_log("get_btc_xmr_exchange:" + str(e), "ERROR", 3)
        return None