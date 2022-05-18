from flask import Flask, jsonify, request
from bitcoinrpc.authproxy import AuthServiceProxy
import threading
import time
from logger import read_log, track_begin, track_end, delete_log
from flask_cors import CORS
from currencies import bitcoin as my_bitcoin
from currencies import monero as my_monero
from currencies import ethereum as my_ethereum
from currencies import blockchain as my_blockchain
from currencies import verge as my_verge
from general import *
from datetime import datetime, timedelta
from config.config import *
import mongodb as mongo
import atexit


app = Flask(__name__)
my_constant.ROOT_PATH = app.root_path
CORS(app)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/get_xmr_address_list', methods=['GET'])
def get_xmr_address_list():
    try:
        track_begin(request)
        result = []
        my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.XMR_WALLET.accounts), LOCAL_ACCOUNT_COUNT)
        for i in range(0, count):
            account = my_constant.XMR_WALLET.accounts[i]
            result.append(str(account.address()))
        track_end(request, str(count))
        return jsonify(result)
    except Exception as e:
        print_log("get_xmr_address_list:" + " " + str(e))
        track_end(request, str(e))
        return jsonify('')


@app.route('/get_eth_account_list', methods=['GET'])
def get_eth_account_list():
    try:
        track_begin(request)
        result = []
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = my_ethereum.get_account_from_private_key(index)
            eth_balance = my_ethereum.get_eth_balance(account.address) - my_constant.ETH_LOCKED_BALANCE[index]['eth']
            tokens = {}
            for token in my_constant.ETH_TOKEN_LIST.keys():
                tokens[token] = my_ethereum.get_token_balance(account.address, token) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token]
            data = {'address': str(account.address), 'ether': eth_balance, 'tokens': tokens}
            result.append(data)
        track_end(request, str(count))
        return jsonify(result)
    except Exception as e:
        print_log("get_eth_account_list:" + " " + str(e), "ERROR", 3)
        track_end(request, str(e))
        return jsonify('')


@app.route('/get_xmr_balance_list', methods=['GET'])
def get_xmr_balance_list():
    try:
        track_begin(request)
        result = []
        my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.XMR_WALLET.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = my_constant.XMR_WALLET.accounts[index]
            balance = float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[index]['locked']
            data = {'address': str(account.address()), 'balance': str(balance)}
            result.append(data)
        track_end(request, str(count))
        return jsonify(result)
    except Exception as e:
        print_log("get_xmr_balance_list:" + " " + str(e), "ERROR", 3)
        track_end(request, str(e))
        return jsonify('')


def run_ex_price_thread():
    try:
        thread = threading.Thread(target=ex_price_thread)
        thread.start()
        thread = threading.Thread(target=miner_fee_thread)
        thread.start()
    except Exception as e:
        print_log("run_ex_price_thread:" + str(e), "ERROR", 3)


def miner_fee_thread():
    while True:
        try:
            my_monero.get_miner_fee_thread()
        except Exception as e:
            print_log("miner_fee_thread:" + str(e), "ERROR", 3)
        time.sleep(300)


def ex_price_thread():
    while True:
        try:
            my_bitcoin.get_usd_ex_price_thread()
            my_monero.get_ex_prices_thread()
            my_ethereum.get_ex_price_thread()
            my_verge.get_ex_prices_thread()
        except Exception as e:
            print_log("ex_price_thread:" + str(e), "ERROR", 3)
        time.sleep(10)


@app.route('/post_review', methods=['GET', 'POST'])
def post_review():
    try:
        track_begin(request)
        content = request.get_json()
        rate = content['rate']
        description = content['description']
        if rate == 0:
            track_end(request, 'rate failed')
            return jsonify('failed')
        if description == "":
            track_end(request, 'description failed')
            return jsonify('failed')
        order_type = content['order_type']
        source_cur = content['source_cur']
        amount = content['amount']
        data = {'rate': rate, 'description': description, 'order_type': order_type,
                'source_cur': source_cur,'amount': amount, 'create_date': datetime.now(), 'is_approved': False}
        mongo.post_review(data)
        track_end(request, 'success')
        return jsonify('success')
    except Exception as e:
        print_log("post_review:" + str(e), "ERROR", 3)
        track_end(request, 'exception')
        return jsonify('exception')


@app.route('/get_reviews', methods=['GET', 'POST'])
def get_reviews():
    try:
        track_begin(request)
        content = request.get_json()
        try:
            rate = content['rate']
        except Exception as e:
            print_log("get_reviews no rate:" + str(e), "DEBUG", 3)
            rate = 0
        try:
            number = content['number']
        except Exception as e:
            print_log("get_reviews no number:" + str(e), "DEBUG", 3)
            number = 0
        try:
            approved = content['is_approved']
        except Exception as e:
            print_log("get_reviews no is_approved:" + str(e), "DEBUG", 3)
            approved = 0
        reviews = mongo.get_reviews(rate, approved)
        track_end(request, 'success')
        if number == 0:
            return jsonify(reviews)
        return jsonify(reviews[:number])
    except Exception as e:
        print_log("get_reviews:" + str(e), "ERROR", 3)
        track_end(request, 'exception')
        return jsonify([])


@app.route('/delete_reviews', methods=['GET'])
def delete_reviews():
    try:
        track_begin(request)
        mongo.delete_all_reviews()
        track_end(request, 'success')
        return jsonify("success")
    except Exception as e:
        print_log("delete_reviews:" + str(e), "ERROR", 3)
        track_end(request, 'exception')
        return jsonify("failed")


@app.route('/create_order', methods=['GET', 'POST'])
def create_order():
    address = ''
    ex_price = 0.0
    btc_miner_fee = 0
    gas_reduce = 0
    dest_miner_fee = 0
    private = ''
    try:
        track_begin(request)
        content = request.get_json()
        order_type = content['order_type']
        gas_price = 0.0
        begin_block = 0
        if content['source_coin'] == "" or content['dest_coin'] == "":
            print_log("Sorry, Your order is not normal format, that's why we declined your order", "ERROR", 3)
            track_end(request, '')
            return jsonify('')
        if content['refund_address'] == "" or content['dst_address'] == "" or \
                content['ex_price'] == 0.0:
            print_log("Sorry, Please Enter Refund Address and Dst address.", "WARNING", 2)
            return jsonify('')
        btc_miner_fee = my_bitcoin.calc_miner_fee()
        if content['source_coin'] == 'BTC':
            address = my_bitcoin.get_btc_new_address()
            if content['dest_coin'] == 'XMR':
                ex_price = my_monero.get_btc_ex_price()
            elif is_key_dict(my_constant.ETH_TOKEN_LIST, content['dest_coin']) is True:
                ex_price = my_ethereum.get_btc_ex_price(content['dest_coin'])
                gas_price = my_ethereum.get_gas_price()
            elif content['dest_coin'] == 'XVG':
                ex_price = my_verge.get_btc_ex_price()
            else:
                print_log("Sorry, Unknown Order.", "ERROR", 3)
                return jsonify('')
        elif content['source_coin'] == 'XMR':
            ex_price = my_monero.get_btc_ex_reverse()
            new_address = my_monero.get_min_wallet_new_address()
            if new_address is None:
                print_log("Sorry, Generation of Monero address was failed.", "ERROR", 3)
                return jsonify('')
            address = new_address['address']
            private = new_address['indexes']
        elif is_key_dict(my_constant.ETH_TOKEN_LIST, content['source_coin']) is True:
            ex_price = my_ethereum.get_btc_ex_reverse(content['source_coin'])
            new_account = my_ethereum.create_wallet()
            address = new_account.address
            private = new_account.privateKey.hex()
            gas_price = my_ethereum.get_gas_price()
            begin_block = int(my_constant.ETH_WEB3.eth.blockNumber) - my_constant.ETH_BEFORE_DETECT_BLOCK
            if content['source_coin'] == 'ETH':
                gas_reduce = my_ethereum.get_eth_limit(gas_price, ETH_GAS_MIN_LIMIT) * my_ethereum.get_btc_ex_reverse('ETH')
            else:
                gas_reduce = my_ethereum.get_eth_limit(gas_price) * my_ethereum.get_btc_ex_reverse('ETH')
        elif content['source_coin'] == 'XVG':
            ex_price = my_verge.get_btc_ex_reverse()
            address = my_verge.get_xvg_new_address()
        else:
            print_log("Sorry, Unknown Order.", "ERROR", 3)
            return jsonify('')
        if content['source_coin'] == 'BTC':
            limit_min = my_bitcoin.get_btc_min(content['dest_coin'])
        else:
            limit_min = my_bitcoin.get_btc_reverse_min(content['source_coin'], ex_price)
        if content['src_amt'] < limit_min:
            print_log("Sorry, Limit " + content['source_coin'] + " amount, Need greater amount than " + str(limit_min), "WARNING", 2)
            track_end(request, '')
            return jsonify('')
        data = {"date": datetime.now(), "order_type": order_type, "end_date": datetime.now(),
                "source_coin": content['source_coin'], "dest_coin": content['dest_coin'], "gas_price": gas_price,
                "dst_address": content['dst_address'], "dst_amt": content['dst_amt'], "address": str(address), "private": str(private),
                "refund_address": content['refund_address'], "refunded_amt": 0, "src_amt": content['src_amt'],
                "status": "created", "details": "Order Created", "user_ip": request.remote_addr, 'gas_reduce': gas_reduce,
                "received": 0, "received_usd": 0, "ex_price": ex_price, 'btc_miner_fee': btc_miner_fee, 'begin_block': begin_block,
                "btc_usd": my_bitcoin.get_btc_usd(), "underpay": False, "exceed": False, "lack": 0, "out_tx_list": []}
        mongo.post_to_db(data)
        message = str(address) + " Order created. Amount:" + str(content['src_amt']) + " Order Type:" + order_type + " Refund Address:" + str(content['refund_address']) + " Dst address:" + str(
            content['dst_address']) + " Dst Amount:" + str(content['dst_amt'])
        print_log(message, "NORMAL", 5)
        if content['dest_coin'] == 'BTC':
            dest_miner_fee = my_bitcoin.get_dest_miner_fee(float(content['src_amt']), ex_price, btc_miner_fee)
        elif content['dest_coin'] == 'XMR':
            dest_miner_fee = my_monero.get_miner_fee()
        elif is_key_dict(my_constant.ETH_TOKEN_LIST, content['dest_coin']) is True:
            if content['dest_coin'] == 'ETH':
                dest_miner_fee = my_ethereum.get_eth_limit(gas_price, ETH_GAS_MIN_LIMIT)
            else:
                gas_reduce = my_ethereum.get_eth_limit(gas_price) * my_ethereum.get_btc_ex_reverse('ETH')
        elif content['dest_coin'] == 'XVG':
            dest_miner_fee = my_verge.get_dest_miner_fee(float(content['src_amt']) * ex_price)
        else:
            pass
    except Exception as err:
        print_log("create_order:" + str(err), "ERROR", 3)
    track_end(request, '')
    result = {'address': str(address), 'ex_price': ex_price, 'btc_miner_fee': btc_miner_fee, 'gas_reduce': gas_reduce, 'dest_miner_fee': dest_miner_fee}
    return jsonify(result)


@app.route('/check_payments', methods=['GET', 'POST'])
def check_payments():
    received = 0
    try:
        track_begin(request)
        content = request.get_json()
        received = mongo.get_order_received(content['address'])
    except Exception as err:
        print_log("check_payments:" + str(err), "ERROR", 3)
    track_end(request, str(received))
    return jsonify(received)


def run_check_transactions_thread():
    thread = threading.Thread(target=my_bitcoin.check_transactions_thread)
    thread.start()
    thread = threading.Thread(target=my_bitcoin.check_completed_orders)
    thread.start()
    thread = threading.Thread(target=my_monero.check_transactions_thread)
    thread.start()
    thread = threading.Thread(target=my_monero.check_completed_orders)
    thread.start()
    thread = threading.Thread(target=my_ethereum.check_transactions_thread)
    thread.start()
    thread = threading.Thread(target=my_ethereum.check_completed_orders)
    thread.start()
    thread = threading.Thread(target=my_verge.check_transactions_thread)
    thread.start()
    thread = threading.Thread(target=my_verge.check_completed_orders)
    thread.start()


@app.route('/get_server_time', methods=['GET'])
def get_server_time():
    try:
        track_begin(request)
        track_end(request, '')
        return jsonify(datetime.now())
    except Exception as e:
        print_log("get_server_time:" + str(e), "ERROR", 3)
    track_end(request, '')
    return jsonify(datetime.now())


@app.route('/rates_limits', methods=['GET'])
def get_price_list():
    try:
        track_begin(request)
        result = []
        ex_price = my_monero.get_btc_ex_price()
        ex_reverse = my_monero.get_btc_ex_reverse()
        btc_miner_fee = my_bitcoin.calc_miner_fee()
        eth_btc = my_ethereum.get_btc_ex_reverse('ETH')
        gas_price = my_ethereum.get_gas_price()
        data = {'name': 'BTC_XMR', 'ex_price': ex_price, 'price_decimal': my_constant.FRONTEND_DECIMALS['XMR']['forward_price'],
                'max': floating(my_monero.get_btc_max_balance(), my_constant.FRONTEND_DECIMALS['XMR']['forward_max']),
                'min': my_bitcoin.get_btc_min('XMR'), 'decimal': my_constant.FRONTEND_DECIMALS['XMR']['forward_amount'],
                'cw_minimum_fee': BTC_CW_MINIMUM_AMOUNT, 'timeout': TIMEOUTS_ORDER['BTC'], 'btc_miner_fee': btc_miner_fee,
                'split_0001': BTC_SPLIT_COUNT_BY_0001_AMOUNT, 'server_time': datetime.now(),
                'split_001': BTC_SPLIT_COUNT_BY_001_AMOUNT, 'split_01': BTC_SPLIT_COUNT_BY_01_AMOUNT, 'gas_reduce': 0,
                'split_1': BTC_SPLIT_COUNT_BY_1_AMOUNT, 'cw_fee': EXCHANGE_FEE_DICT['XMR'] / 2, 'dest_miner_fee': my_monero.get_miner_fee()}

        result.append(data)
        data = {'name': 'XMR_BTC', 'ex_price': ex_reverse, 'price_decimal': my_constant.FRONTEND_DECIMALS['XMR']['reverse_price'],
                'max': my_bitcoin.get_btc_max_balance(ex_reverse, my_constant.FRONTEND_DECIMALS['XMR']['reverse_max']),
                'min': my_bitcoin.get_btc_reverse_min('XMR', ex_reverse), 'server_time': datetime.now(),
                'decimal': my_constant.FRONTEND_DECIMALS['XMR']['reverse_amount'], 'gas_reduce': 0,
                'timeout': TIMEOUTS_ORDER['XMR'], 'cw_fee': (EXCHANGE_FEE_DICT['XMR'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 2,
                'btc_miner_fee': btc_miner_fee, 'dest_miner_fee': btc_miner_fee, 'cw_minimum_fee': BTC_CW_MINIMUM_AMOUNT}
        result.append(data)
        ex_price = my_verge.get_btc_ex_price()
        ex_reverse = my_verge.get_btc_ex_reverse()
        data = {'name': 'BTC_XVG', 'ex_price': ex_price,
                'price_decimal': my_constant.FRONTEND_DECIMALS['XVG']['forward_price'],
                'max': floating(my_verge.get_btc_max_balance(), my_constant.FRONTEND_DECIMALS['XVG']['forward_max']),
                'min': my_bitcoin.get_btc_min('XVG'), 'decimal': my_constant.FRONTEND_DECIMALS['XVG']['forward_amount'],
                'cw_minimum_fee': BTC_CW_MINIMUM_AMOUNT, 'timeout': TIMEOUTS_ORDER['BTC'],
                'btc_miner_fee': btc_miner_fee, 'server_time': datetime.now(),
                'split_0001': BTC_SPLIT_COUNT_BY_0001_AMOUNT,
                'split_001': BTC_SPLIT_COUNT_BY_001_AMOUNT, 'split_01': BTC_SPLIT_COUNT_BY_01_AMOUNT, 'gas_reduce': 0,
                'split_1': BTC_SPLIT_COUNT_BY_1_AMOUNT, 'cw_fee': EXCHANGE_FEE_DICT['XVG'] / 2,
                'dest_miner_fee': my_verge.calc_miner_fee()}

        result.append(data)
        data = {'name': 'XVG_BTC', 'ex_price': ex_reverse,
                'price_decimal': my_constant.FRONTEND_DECIMALS['XVG']['reverse_price'],
                'max': my_bitcoin.get_btc_max_balance(ex_reverse, my_constant.FRONTEND_DECIMALS['XVG']['reverse_max']),
                'min': my_bitcoin.get_btc_reverse_min('XVG', ex_reverse),
                'decimal': my_constant.FRONTEND_DECIMALS['XVG']['reverse_amount'], 'gas_reduce': 0,
                'timeout': TIMEOUTS_ORDER['XVG'], 'server_time': datetime.now(),
                'cw_fee': (EXCHANGE_FEE_DICT['XVG'] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 2,
                'btc_miner_fee': btc_miner_fee, 'dest_miner_fee': btc_miner_fee,
                'cw_minimum_fee': BTC_CW_MINIMUM_AMOUNT}
        result.append(data)

        for token in my_constant.ETH_TOKEN_LIST.keys():
            ex_price = my_ethereum.get_btc_ex_price(token)
            ex_reverse = my_ethereum.get_btc_ex_reverse(token)
            if token == 'ETH':
                gas = my_ethereum.get_eth_limit(gas_price, ETH_GAS_MIN_LIMIT)
                gas_reduce = gas * eth_btc
                data = {'name': 'BTC_' + token, 'ex_price': ex_price,
                        'price_decimal': my_constant.FRONTEND_DECIMALS[token]['forward_price'],
                        'max': floating(my_ethereum.get_btc_max_balance(token),
                                        my_constant.FRONTEND_DECIMALS[token]['forward_max']),
                        'min': my_bitcoin.get_btc_min(token), 'decimal': my_constant.FRONTEND_DECIMALS[token]['forward_amount'],
                        'timeout': TIMEOUTS_ORDER['BTC'], 'btc_miner_fee': btc_miner_fee,
                        'split_0001': BTC_SPLIT_COUNT_BY_0001_AMOUNT, 'server_time': datetime.now(),
                        'split_001': BTC_SPLIT_COUNT_BY_001_AMOUNT, 'split_01': BTC_SPLIT_COUNT_BY_01_AMOUNT,
                        'gas_reduce': 0, 'cw_minimum_fee': BTC_CW_MINIMUM_AMOUNT,
                        'split_1': BTC_SPLIT_COUNT_BY_1_AMOUNT, 'cw_fee': EXCHANGE_FEE_DICT[token] / 2,
                        'dest_miner_fee': gas}
            else:
                gas_reduce = my_ethereum.get_eth_limit(gas_price) * eth_btc
                data = {'name': 'BTC_' + token, 'ex_price': ex_price,
                        'price_decimal': my_constant.FRONTEND_DECIMALS[token]['forward_price'],
                        'max': floating(my_ethereum.get_btc_max_balance(token),
                                        my_constant.FRONTEND_DECIMALS[token]['forward_max']),
                        'min': my_bitcoin.get_btc_min(token), 'decimal': my_constant.FRONTEND_DECIMALS[token]['forward_amount'],
                        'timeout': TIMEOUTS_ORDER['BTC'], 'btc_miner_fee': btc_miner_fee,
                        'split_0001': BTC_SPLIT_COUNT_BY_0001_AMOUNT, 'server_time': datetime.now(),
                        'split_001': BTC_SPLIT_COUNT_BY_001_AMOUNT, 'split_01': BTC_SPLIT_COUNT_BY_01_AMOUNT,
                        'gas_reduce': gas_reduce, 'cw_minimum_fee': BTC_CW_MINIMUM_AMOUNT,
                        'split_1': BTC_SPLIT_COUNT_BY_1_AMOUNT, 'cw_fee': EXCHANGE_FEE_DICT[token] / 2,
                        'dest_miner_fee': 0}
            result.append(data)
            data = {'name': token + '_BTC',
                    'ex_price': ex_reverse, 'price_decimal': my_constant.FRONTEND_DECIMALS[token]['reverse_price'],
                    'max': my_bitcoin.get_btc_max_balance(ex_reverse, my_constant.FRONTEND_DECIMALS[token]['reverse_max']),
                    'min': my_bitcoin.get_btc_reverse_min(token, ex_reverse), 'server_time': datetime.now(),
                    'decimal': my_constant.FRONTEND_DECIMALS[token]['reverse_amount'], 'gas_reduce': gas_reduce,
                    'timeout': TIMEOUTS_ORDER[token], 'cw_fee': (EXCHANGE_FEE_DICT[token] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 2,
                    'btc_miner_fee': btc_miner_fee, 'dest_miner_fee': btc_miner_fee, 'cw_minimum_fee': BTC_CW_MINIMUM_AMOUNT}
            result.append(data)
        track_end(request, '')
        return jsonify(result)
    except Exception as err:
        print_log("get_price_list:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify([])


@app.route('/get_order_result', methods=['GET', 'POST'])
def get_order_result():
    try:
        track_begin(request)
        content = request.get_json()
        address = content['address']
        result = mongo.pull_order(address)
        track_end(request, '')
        return jsonify(result)
    except Exception as err:
        print_log("get_order_result:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify([])


@app.route('/get_last_completed_orders', methods=['GET'])
def get_last_completed_orders():
    try:
        track_begin(request)
        delta = timedelta(hours=240)
        #result = mongo.pull_by_date(datetime.now() - delta, "completed")
        result = mongo.pull_by_date(datetime(2021, 6, 30, 22), "completed")
        track_end(request, '')
        if len(result) <= 0:
            return jsonify([])
        start = len(result) - 10
        if start < 0:
            start = 0
        res = []
        for i in range(start, len(result)):
            res.append(result[i])
        return jsonify(res)
    except Exception as err:
        print_log("get_last_completed_orders:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify([])


def get_confirm_time(order):
    try:
        confirm_time = 0
        for item in order['out_tx_list']:
            if confirm_time < item['took_time']:
                confirm_time = item['took_time']
        return confirm_time
    except Exception as e:
        print_log("get_confirm_time:" + str(e), "ERROR", 3)
        return 0


def get_first_start_time(order):
    try:
        start_time = datetime.now()
        if len(order['out_tx_list']) <= 0:
            return start_time
        start_time = order['out_tx_list'][0]['time']
        return start_time
    except Exception as e:
        print_log("get_first_start_time:" + str(e), "ERROR", 3)
        return datetime.now()


@app.route('/get_24h_volume_and_time', methods=['GET'])
def get_24h_volume_and_time():
    res = {'volume': 0, 'time': 0}
    try:
        track_begin(request)
        delta = timedelta(hours=240)
        #result = mongo.pull_by_date(datetime.now() - delta, "completed")
        result = mongo.pull_by_date(datetime(2021, 6, 30, 22), "completed")
        btc_volume = 0
        processing_time = 0
        for item in result:
            if item['source_coin'] == "BTC":
                btc_volume += item['received']
            else:
                btc_volume += item['dst_amt']
            confirm_time = get_confirm_time(item)
            start_time = get_first_start_time(item)
            total_time = item['end_date'] - start_time
            processing_time += (total_time.total_seconds() - confirm_time)
        if len(result) != 0:
            processing_time /= (len(result) * 60)
        track_end(request, '')
        res = {'volume': btc_volume, 'time': processing_time}
        return jsonify(res)
    except Exception as err:
        print_log("get_24h_volume_and_time:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify(res)


@app.route('/get_btc_balance', methods=['GET'])
def get_btc_balance():
    try:
        track_begin(request)
        btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
        balance = btc_rpc_connection.getbalance()
    except Exception as err:
        balance = 'check'
        print_log("get_btc_balance:" + str(err), "ERROR", 3)
    track_end(request, str(balance))
    return str(balance)


@app.route('/get_xvg_balance', methods=['GET'])
def get_xvg_balance():
    try:
        track_begin(request)
        conn = my_verge.get_connection()
        balance = conn.getbalance()
    except Exception as err:
        balance = 'check'
        print_log("get_xvg_balance:" + str(err), "ERROR", 3)
    track_end(request, str(balance))
    return str(balance)


@app.route('/all_orders')
def get_all_orders():
    try:
        track_begin(request)
        a = mongo.get_all_orders()
        track_end(request, '')
        return jsonify(a)
    except Exception as err:
        print_log("all_orders:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify([])


def get_total_usd_from_orders(orders):
    total = 0.0
    for order in orders:
        try:
            received_usd = order['received_usd']
        except Exception as e:
            print_log("get_total_btc_usd:" + str(e), "WARNING", 2)
            btc_list = order['out_tx_list']
            rate = order['btc_usd']
            amount = 0.0
            for item in btc_list:
                if item['status'] != 'completed':
                    continue
                amount += item['amount']
            received_usd = floating(rate * amount, 2)
            mongo.update_received_usd(order['address'], received_usd)
        total += received_usd
    return floating(total, 2)


@app.route('/total_btc_usd')
def get_total_btc_usd():
    try:
        track_begin(request)
        orders = mongo.get_all_orders()
        total = get_total_usd_from_orders(orders)
        total = round(total, 2)
        track_end(request, "$" + str(total) + "USD")
        return jsonify("$" + str(total) + "USD")
    except Exception as err:
        print_log("total_btc_usd:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify('')


@app.route('/get_btc_new_address_from_blockchain')
def get_btc_new_address_from_blockchain():
    try:
        track_begin(request)
        new_address = my_blockchain.get_new_address_from_blockchain()
        track_end(request, new_address)
        return jsonify(new_address)
    except Exception as err:
        print_log("get_btc_new_address_from_blockchain:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify('')


@app.route('/view_log')
def view_log():
    try:
        track_begin(request)
        data = read_log()
        track_end(request, '')
        return jsonify(data)
    except Exception as err:
        print_log("view_log:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify('')


@app.route('/completed_orders')
def completed_orders():
    try:
        track_begin(request)
        orders = mongo.pull_by_status('completed')
        track_end(request, '')
        return jsonify(orders)
    except Exception as err:
        print_log("completed_orders:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify('')


@app.route('/delete_log')
def remove_log():
    try:
        track_begin(request)
        data = delete_log()
        track_end(request, '')
        return jsonify(data)
    except Exception as err:
        print_log("delete_log:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify('')


@app.route('/canceled_orders')
def canceled_orders():
    try:
        track_begin(request)
        orders = mongo.pull_by_status('canceled')
        track_end(request, '')
        return jsonify(orders)
    except Exception as err:
        print_log("canceled_orders:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify('')


@app.route('/created_orders')
def created_orders():
    try:
        track_begin(request)
        orders = mongo.pull_by_status('created')
        track_end(request, '')
        return jsonify(orders)
    except Exception as err:
        print_log("created_orders:" + str(err), "ERROR", 3)
        track_end(request, '')
        return jsonify('')


@app.route('/delete_all')
def delete_all():
    try:
        track_begin(request)
        result = mongo.delete_all_orders()
        return jsonify(result)
    except Exception as e:
        print_log("delete_all:" + str(e), "ERROR", 3)
        track_end(request, '')
        return jsonify(0)


def run_loading_balancing_wallet():
    try:
        thread = threading.Thread(target=my_monero.load_balancing_wallet)
        thread.start()
        thread = threading.Thread(target=my_ethereum.load_balancing_wallet)
        thread.start()
        thread = threading.Thread(target=my_bitcoin.load_balancing)
        thread.start()
        thread = threading.Thread(target=my_verge.load_balancing)
        thread.start()
    except Exception as e:
        print_log("run_loading_balancing_wallet:" + str(e), "ERROR", 3)


def run_create_wallet_addresses():
    try:
        my_monero.create_wallet_addresses()
        my_ethereum.create_wallet_addresses()
    except Exception as e:
        print_log("run_create_wallet_addresses:" + str(e), "ERROR", 3)


def close_functions():
    try:
        print_log("Exit Flask App", "ALARM", 5)
        if my_constant.BLOCKCHAIN_DRIVER is not None:
            my_constant.BLOCKCHAIN_DRIVER.quit()
    except Exception as e:
        print_log("close_functions: " + str(e), "WARNING", 3)


run_ex_price_thread()
run_create_wallet_addresses()
run_loading_balancing_wallet()
run_check_transactions_thread()
atexit.register(close_functions)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
    close_functions()
