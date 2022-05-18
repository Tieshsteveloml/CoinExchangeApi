import requests
import time
import threading
import random
from bitcoinrpc.authproxy import AuthServiceProxy
from general import *
from currencies import monero as my_monero
from currencies import verge as my_verge
from currencies import ethereum as my_ethereum
from currencies import blockchain as my_blockchain
from datetime import datetime, timedelta
from config.config import *
import mongodb as mongo
from currencies.exchange import get_btc_usd_exchange


def get_usd_ex_price_thread():
    try:
        my_constant.EX_BTC_USD = floating(get_btc_usd_exchange(), 2)
        return my_constant.EX_BTC_USD
    except Exception as e:
        print_log("bitcoin:get_usd_ex_price_thread:" + str(e), "ERROR", 3)
        return my_constant.EX_BTC_USD


def get_btc_usd():
    while True:
        try:
            if my_constant.EX_BTC_USD == 0.0:
                my_constant.EX_BTC_USD = get_usd_ex_price_thread()
            return my_constant.EX_BTC_USD
        except Exception as e:
            print_log("get_btc_usd:" + str(e), "ERROR", 3)
            time.sleep(1)


def get_btc_decimals():
    return my_constant.BITCOIN_DECIMALS


def get_btc_min(token, decimals=4):
    if IS_STATIC_MIN:
        return LIMIT_MIN['BTC']
    return floating(BTC_CW_MINIMUM_AMOUNT * 100 / (EXCHANGE_FEE_DICT[token] / 2), decimals)


def get_btc_reverse_min(token, ex_price):
    if IS_STATIC_MIN:
        return LIMIT_MIN[token]
    if ex_price == 0:
        return 0
    amount = BTC_CW_MINIMUM_AMOUNT * 100 / ((EXCHANGE_FEE_DICT[token] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 2)
    decimals = my_constant.FRONTEND_DECIMALS[token]['reverse_min']
    return floating(amount / ex_price, decimals)


def get_btc_max_balance(ex_price, decimals):
    try:
        if ex_price == 0:
            return 0
        orders = mongo.pull_by_status('created', dest_coin='BTC')
        amount = 0
        for order in orders:
            for item in order['out_tx_list']:
                if item['step'] >= 2:
                    continue
                amount += item['dst_amt']
        amount += my_constant.BITCOIN_CW_AMOUNT_FEE['amount']
        btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
        balance = btc_rpc_connection.getbalance()
        amount = float(balance) - amount
        return floating(amount / ex_price, decimals)
    except Exception as e:
        print_log("get_btc_max_balance:" + str(e), "ERROR", 3)
        return 0


def calc_min_confirms(amount):
    if amount < 1:
        return UNDER_1_BTC
    if amount < 5:
        return UNDER_5_BTC
    if amount < 10:
        return UNDER_10_BTC
    return OVER_10_BTC


def crypto_confirm_payment(order, tx):
    while True:
        try:
            if order['source_coin'] == 'XMR':
                result = my_monero.confirm_payment(order, tx)
            elif is_key_dict(my_constant.ETH_TOKEN_LIST, order['source_coin']) is True:
                result = my_ethereum.confirm_payment(order, tx)
            elif order['source_coin'] == 'XVG':
                result = my_verge.confirm_payment(order, tx)
            else:
                return None
            return result
        except Exception as e:
            print_log("crypto_confirm_payment:" + str(e), "ERROR", 3)
            time.sleep(1)


def confirm_payment(order, tx):
    while True:
        try:
            if mongo.is_existed_out_tx_order(order['address'], tx) is False:
                return 0.0
            btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
            item = btc_rpc_connection.gettransaction(tx['tx_id'])
            amount = 0
            for detail in item['details']:
                if detail['category'] != 'receive':
                    continue
                if detail['address'] == order['address']:
                    amount = detail['amount']
                    break
            if amount == 0:
                time.sleep(1)
                continue
            amount = floating(amount, 8)
            if int(item['confirmations']) < calc_min_confirms(amount):
                time.sleep(1)
                continue
            tx['confirmations'] = int(item['confirmations'])
            received = amount
            if received == 0:
                time.sleep(1)
                continue
            return floating(received, 8)
        except Exception as e:
            print_log(str(e), "ERROR", 3)
            time.sleep(1)


def get_vout_from_unspend_tx(address, tx_id):
    max_tries = 10
    tries = 0
    while tries < max_tries:
        try:
            btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
            tx_list = btc_rpc_connection.listunspent(1, my_constant.BITCOIN_MAXIMUM_UNUSED_CONFIRM, [address])
            for item in tx_list:
                if item['txid'] != tx_id:
                    continue
                return item['vout']
            break
        except Exception as e:
            print_log("get_vout_from_unspend_tx: " + str(e), "ERROR", 5)
            time.sleep(5)
    return -1


def send_btc(inputs, outputs, fee):
    my_constant.BTC_MUTEX.acquire()
    try:
        btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
        res = btc_rpc_connection.settxfee(fee)
        if res is False:
            print_log("send_btc: set mining fee: " + str(fee) + " FAILED", "ERROR", 5)
            my_constant.BTC_MUTEX.release()
            return None
        raw_tx = btc_rpc_connection.createrawtransaction(inputs, outputs)
        signed_hex = btc_rpc_connection.signrawtransactionwithwallet(raw_tx)
        if signed_hex['complete'] is False:
            print_log("send_btc: signing transaction failed  error:" + signed_hex['errors'], "NORMAL", 5)
            my_constant.BTC_MUTEX.release()
            return None
        tx_cw = btc_rpc_connection.sendrawtransaction(signed_hex['hex'])
        my_constant.BTC_MUTEX.release()
        return tx_cw
    except Exception as e:
        print_log("send_btc: " + str(e) + " inputs:" + str(inputs) + " outputs:" + str(outputs), "WARNING", 3)
        my_constant.BTC_MUTEX.release()
        return None


def send_btc_to_address(tx_id, to_address, amount, vout, is_raw_tx=False, fee=0):
    result = []
    my_constant.BTC_MUTEX.acquire()
    try:
        btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
        if is_raw_tx:
            if fee == 0:
                fee = calc_miner_fee(BTC_MINER_FEE_BLOCK_SIZE)
            if fee >= amount:
                fee = floating(amount * BTC_LOW_FEE_MIN / 100.0, 8)
                if fee < 0.00001:
                    fee = 0.00001
                print_log("send_btc_to_address: recalculated mining fee: " + str(fee), "NORMAL", 5)
            res = btc_rpc_connection.settxfee(fee)
            if res is False:
                print_log("send_btc_to_address: set mining fee: " + str(fee) + " FAILED", "ERROR", 5)
                my_constant.BTC_MUTEX.release()
                return None
            inputs = [{'txid': tx_id, 'vout': vout}]
            outputs = [{to_address: floating(amount - fee, 8)}]
            raw_tx = btc_rpc_connection.createrawtransaction(inputs, outputs)
            signed_hex = btc_rpc_connection.signrawtransactionwithwallet(raw_tx)
            if signed_hex['complete'] is False:
                print_log("send_btc_to_address: signing transaction failed  error:" + signed_hex['errors'], "NORMAL", 5)
                my_constant.BTC_MUTEX.release()
                return None
            tx_cw = btc_rpc_connection.sendrawtransaction(signed_hex['hex'])
            my_constant.BTC_MUTEX.release()
            result.append(tx_cw)
            return result
        else:
            tx_cw = btc_rpc_connection.sendtoaddress(to_address, amount, '', '', True)
            my_constant.BTC_MUTEX.release()
            result.append(tx_cw)
            return result
    except Exception as e:
        print_log("send_btc_to_address: " + str(e), "WARNING", 3)
        my_constant.BTC_MUTEX.release()
        return None


def calc_mining_fee(block_size):
    while True:
        try:
            url = my_constant.BLOCKSTREAM_API_URL + 'fee-estimates'
            response = requests.get(url)
            fee_est = response.json()
            if BTC_MINING_FEE_LEVEL == 1:
                fee_per_byte = calc_estimate_fee(fee_est, BTC_FEE_HIGH_LIMIT)
                if fee_per_byte == -1:
                    time.sleep(1)
                    continue
            elif BTC_MINING_FEE_LEVEL == 2:
                fee_per_byte = calc_estimate_fee(fee_est, BTC_FEE_MEDIUM_LIMIT)
                if fee_per_byte == -1:
                    time.sleep(1)
                    continue
            else:
                fee_per_byte = calc_estimate_fee(fee_est, BTC_FEE_LOW_LIMIT)
                if fee_per_byte == -1:
                    time.sleep(1)
                    continue
                if fee_per_byte >= 100:
                    fee_per_byte = 70.0
            fee = fee_per_byte * block_size / pow(10.0, 8)
            if fee < 0.00001000:
                fee = 0.00001
            print_log("calc_mining_fee size: " + str(block_size) + " fee: " + str(fee), "NORMAL", 5)
            return floating(fee, 8)
        except Exception as err:
            print_log("calc_mining_fee:" + str(err), "ERROR", 3)
            time.sleep(1)


def calc_miner_fee(block_size=BTC_MINER_FEE_BLOCK_SIZE, level=BTC_MINER_FEE_CONFIRMATION_TARGET):
    while True:
        try:
            btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
            fee_per_k_byte = btc_rpc_connection.estimatesmartfee(level, BTC_MINER_FEE_ESTIMATE_MODE)
            my_constant.BTC_FEE_RATE = float(fee_per_k_byte['feerate'])
        except Exception as err:
            print_log("calc_miner_fee:" + str(err), "ERROR", 3)
        fee = my_constant.BTC_FEE_RATE / 1000 * block_size
        if fee < 0.00001000:
            fee = 0.00001
        print_log("calc_miner_fee size: " + str(block_size) + " fee: " + str(fee), "NORMAL", 5)
        return floating(fee, 8)


def get_unconfirmed_count(tx):
    try:
        btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
        min_conf = calc_min_confirms(floating(tx['amount'], 8))
        lists = btc_rpc_connection.listunspent(0, min_conf - 1, [tx['address']])
        return len(lists)
    except Exception as e:
        print_log("get_unconfirmed_count:" + str(e), "ERROR", 3)
        return 0


def get_unconfirmed_count_from_sender(tx_data):
    retries = 0
    result = 0
    while retries < 10:
        try:
            vins = tx_data['vin']
            max_un = 0
            btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
            for vin in vins:
                raw_tx = btc_rpc_connection.getrawtransaction(vin['tx_id'], True)
                if raw_tx is None:
                    continue
                sender_address = ''
                for out_tx in raw_tx['vout']:
                    if out_tx['n'] != vin['vout']:
                        continue
                    if len(out_tx['addresses']) <= 0:
                        continue
                    sender_address = out_tx['addresses'][0]
                if sender_address == '':
                    continue
                address_url = my_constant.BLOCKSTREAM_API_URL + "address/" + sender_address
                sender_info = requests.get(address_url).json()
                unconfirmed = sender_info['mempool_stats']['tx_count']
                if max_un < unconfirmed:
                    max_un = unconfirmed
            print_log("get_unconfirmed_count_from_sender: unconfirmed:" + str(max_un), "DEBUG", 5)
            if max_un > 1:
                return max_un
            if max_un == 0:
                return 1
            if result < max_un:
                result = max_un
            time.sleep(0.3)
        except Exception as e:
            print_log("get_unconfirmed_count_from_sender:" + str(e), "ERROR", 3)
            time.sleep(1)
        retries += 1
    return result


def get_fee(tx):
    while True:
        try:
            btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
            medium_fee = btc_rpc_connection.estimatesmartfee(6, BTC_MINER_FEE_ESTIMATE_MODE)
            raw_tx = btc_rpc_connection.getrawtransaction(tx['txid'], True)
            satoshi = math.fabs(float(tx['fee']) * 100000000) // raw_tx['size'] + 1
            if float(satoshi) >= float(medium_fee['feerate']) * 100000000 / 1000:
                unconfirmed = get_unconfirmed_count_from_sender(raw_tx)
                if unconfirmed > 1:
                    print_log("get_fee: detected several unconfirmed transactions, count:" + str(unconfirmed), "NORMAL",
                              5)
                    priority = 'low ' + str(satoshi) + ' sats/vB'
                else:
                    priority = 'high ' + str(satoshi)+' sats/vB'
            else:
                priority = 'low ' + str(satoshi)+' sats/vB'
            break
        except Exception as err:
            print_log("get_fee:" + str(err), "ERROR", 3)
            time.sleep(1)
    return priority


def calc_estimate_fee(fee_est, tries):
    fee_per_byte = -1
    while tries >= 0:
        try:
            fee_per_byte = fee_est[str(tries)]
            break
        except Exception as e:
            print_log('calc_estimate_fee ' + str(e))
            tries -= 1
    return fee_per_byte


def get_amount_from_tx(address, tx):
    try:
        for item in tx['details']:
            if item['category'] != 'receive':
                continue
            if item['address'] != address:
                continue
            return floating(item['amount'], get_btc_decimals())
        return 0
    except Exception as e:
        print_log("get_amount_from_tx:" + str(e), "ERROR", 3)
        return 0


def make_transaction(address, tx, dest_coin='XMR', ex_price=0):
    try:
        amount = get_amount_from_tx(address, tx)
        if amount <= 0:
            return None
        fee = get_fee(tx)
        confirm = int(tx['confirmations'])
        min_conf = calc_min_confirms(amount)
        if confirm >= min_conf:
            status = "confirmed"
        else:
            status = "created"
        if dest_coin == 'XMR':
            dest_miner_fee = my_monero.get_dest_miner_fee(amount * ex_price)
        elif dest_coin == 'XVG':
            dest_miner_fee = my_verge.get_dest_miner_fee(amount * ex_price)
        else:
            dest_miner_fee = 0
        data = {'address': address, 'amount': amount, 'refunded': 0.0, 'confirming_time': '',
                'confirmations': confirm, 'tx_id': tx['txid'], 'min_conf': min_conf, 'sent': 0,
                'time': datetime.now(), 'miner_fee': fee, 'processing_time': '',
                'step': 0, 'status': status, 'received': 0.0, 'split': {'steps': 0, 'lists':[]},
                'comment': '', 'dst_amt': 0.0, 'took_time': 0, 'tx_cw': {'amount': 0, 'address': '', 'tx_id': ''},
                'tx': [], 'tx_ref': [], 'dest_miner_fee': dest_miner_fee}
        return data
    except Exception as e:
        print_log("make_transaction:" + str(e), "ERROR", 3)
        return None


def insert_and_append_list_btc_tx(order, tx):
    try:
        tx_list = order['out_tx_list']
        dest_coin = order['dest_coin']
        ex_price = order['ex_price']
        for i in range(0, len(tx_list)):
            if tx_list[i]['tx_id'] == tx['txid']:
                if tx_list[i]['confirmations'] < int(tx['confirmations']):
                    tx_list[i]['confirmations'] = int(tx['confirmations'])
                return tx_list[i]
        cust_tx = make_transaction(order['address'], tx, dest_coin, ex_price)
        if cust_tx is None:
            return None
        tx_list.append(cust_tx)
        return cust_tx
    except Exception as e:
        print_log("insert_and_append_list_btc_tx:" + str(e), "ERROR", 3)
        return None


def start_transaction_thread(order, btc_tx):
    if btc_tx['step'] >= 8:
        return
    if order['source_coin'].upper() != 'BTC':
        return
    if order['dest_coin'].upper() == 'XMR':
        thread = threading.Thread(target=my_monero.check_conf, args=(order, btc_tx))
    elif is_key_dict(my_constant.ETH_TOKEN_LIST, order['dest_coin']) is True:
        thread = threading.Thread(target=my_ethereum.check_conf, args=(order, btc_tx))
    elif order['dest_coin'].upper() == 'XVG':
        thread = threading.Thread(target=my_verge.check_conf, args=(order, btc_tx))
    else:
        return
    thread.start()


def check_completed_orders():
    while True:
        try:
            start_date = datetime.now() - timedelta(days=OLD_ORDER_CHECK_DAYS)
            orders = mongo.pull_by_date(start_date, ['completed', 'canceled'], 'BTC')
            if orders is None:
                time.sleep(0.1)
                continue
            for order in orders:
                lists = get_tx_list_by_address(order['address'])
                for item in lists:
                    try:
                        my_constant.TRANSACTION_MUTEX.acquire()
                        order = mongo.find_order_by_address(order['address'], 'BTC')
                        if order is None:
                            my_constant.TRANSACTION_MUTEX.release()
                            continue
                        btc_tx = insert_and_append_list_btc_tx(order, item)
                        if btc_tx is None:
                            continue
                        mongo.update_out_tx_list(order['address'], order['out_tx_list'])
                        start_transaction_thread(order, btc_tx)
                        my_constant.TRANSACTION_MUTEX.release()
                        time.sleep(0.01)
                    except Exception as e:
                        if my_constant.TRANSACTION_MUTEX.locked():
                            my_constant.TRANSACTION_MUTEX.release()
                        print_log("check_completed_orders locked1:" + str(e), "ERROR", 3)
                        continue
                # orders.append(order)
            time.sleep(10)
        except Exception as e:
            print_log("check_completed_orders:" + str(e), "ERROR", 3)
            time.sleep(10)


def get_tx_list_by_address(address):
    result = []
    try:
        btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
        lists = btc_rpc_connection.listreceivedbyaddress(0, True, True, address)
        if lists is None:
            return result
        for tx_list in lists:
            for item in tx_list['txids']:
                tx = btc_rpc_connection.gettransaction(item)
                if tx is None:
                    continue
                result.append(tx)
        return result
    except Exception as e:
        print_log("get_tx_list_by_address:" + str(e), "ERROR", 3)
        return result


def check_transactions_thread():
    while True:
        try:
            orders = mongo.pull_by_status('created', 'BTC')
            if orders is None:
                time.sleep(0.1)
                continue
            for order in orders:
                lists = get_tx_list_by_address(order['address'])
                for item in lists:
                    try:
                        my_constant.TRANSACTION_MUTEX.acquire()
                        finding_order = mongo.find_order_by_address(order['address'], 'BTC')
                        if finding_order is None:
                            my_constant.TRANSACTION_MUTEX.release()
                            continue
                        order = finding_order
                        btc_tx = insert_and_append_list_btc_tx(order, item)
                        if btc_tx is None:
                            continue
                        mongo.update_out_tx_list(order['address'], order['out_tx_list'])
                        start_transaction_thread(order, btc_tx)
                        my_constant.TRANSACTION_MUTEX.release()
                    except Exception as e:
                        if my_constant.TRANSACTION_MUTEX.locked():
                            my_constant.TRANSACTION_MUTEX.release()
                        print_log("check_transactions_thread locked1:" + str(e), "ERROR", 3)
                        continue
                if len(order['out_tx_list']) == 0:
                    mongo.process_canceled_order(order, TIMEOUTS_ORDER['BTC'])
                else:
                    res = update_order_amounts(order, lists)
                    if res == 0:
                        mongo.update_order_status(order['address'], 'completed', 'Completed order.')
                    elif res == 1:
                        mongo.process_canceled_order(order, TIMEOUTS_ORDER['BTC'])
            time.sleep(0.1)
        except Exception as e:
            print_log("check_transactions_thread:" + str(e), "ERROR", 3)
            time.sleep(0.1)


def bitcoin_confirm(address, tx_id):
    try:
        btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
        item = btc_rpc_connection.gettransaction(tx_id)
        for detail in item['details']:
            if detail['address'] == address and int(item['confirmations']) > 0:
                return True
        return False
    except Exception as e:
        print_log("bitcoin_confirm:" + str(e), "ERROR", 3)
        return False


def get_btc_new_address():
    while True:
        try:
            btc_rpc = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=120)
            address = btc_rpc.getnewaddress()
            break
        except Exception as err:
            print_log("get_btc_new_address:" + str(err), "ERROR", 3)
            pass
    return address


def bitcoin_split_step(primary_address, primary_tx, address, tx_id, amount, const_params, step):
    lock_value = {'address': address, 'tx_id': tx_id, 'amount': amount}
    try:
        my_constant.BTC_OUTPUTS_MUTEX.acquire()
        if is_key_dict(my_constant.BTC_LOCKED_BALANCE, my_constant.BTC_SPLITTING_KEY) is False:
            my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY] = []
        if lock_value in my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY] is False:
            my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY].append(lock_value)
        my_constant.BTC_OUTPUTS_MUTEX.release()
        const_params['mutex'].acquire()
        if step >= const_params['steps']:
            print_log("bitcoin_split_step finished, primary_address:" + primary_address + " primary_tx:" + primary_tx, "DEBUG", 1)
            const_params['mutex'].release()
            my_constant.BTC_OUTPUTS_MUTEX.acquire()
            if lock_value in my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY]:
                my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY].remove(lock_value)
            my_constant.BTC_OUTPUTS_MUTEX.release()
            return
        while True:
            confirmed = bitcoin_confirm(address, tx_id)
            if confirmed:
                break
            time.sleep(1)

        vout = get_vout_from_unspend_tx(address, tx_id)
        if vout < 0:
            print_log("bitcoin_split_step already spent, address:" + address + " tx_id:" + tx_id, "ERROR", 3)
            const_params['mutex'].release()
            my_constant.BTC_OUTPUTS_MUTEX.acquire()
            if lock_value in my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY]:
                my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY].remove(lock_value)
            my_constant.BTC_OUTPUTS_MUTEX.release()
            return
        real_amount = amount - const_params['miner_fee']
        if real_amount <= my_constant.BITCOIN_SPLITTING_MINIMUM_AMOUNT:
            print_log("bitcoin_split_step too small amount, address:" + address + " tx_id:" + tx_id, "ERROR", 3)
            const_params['mutex'].release()
            my_constant.BTC_OUTPUTS_MUTEX.acquire()
            if lock_value in my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY]:
                my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY].remove(lock_value)
            my_constant.BTC_OUTPUTS_MUTEX.release()
            return
        split_percent = random.randint(40, 60)
        amount_1 = floating(real_amount * split_percent / 100, 8)
        amount_2 = floating(real_amount - amount_1, 8)
        new_address_1 = get_btc_new_address()
        new_address_2 = get_btc_new_address()
        inputs = [{'txid': tx_id, 'vout': vout}]
        outputs = [{new_address_1: amount_1}, {new_address_2: amount_2}]
        print_log("splitting: amount:" + str(amount) + " miner_fee:" + str(const_params['miner_fee']) + " outputs:" + str(outputs), 'WARNING', 3)
        tx_cw = send_btc(inputs, outputs, const_params['miner_fee'])
        if tx_cw is None:
            print_log(
                "bitcoin_split_step GENERAL transfer error: already spent, step:" + str(step) + " primary_address:" + primary_address + " primary_tx:" + primary_tx,
                "ERROR", 3)
            const_params['mutex'].release()
            my_constant.BTC_OUTPUTS_MUTEX.acquire()
            if lock_value in my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY]:
                my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY].remove(lock_value)
            my_constant.BTC_OUTPUTS_MUTEX.release()
            return
        split_tx = {'step': step, 'amount_1': amount_1, 'address_1': new_address_1,
                    'amount_2': amount_2, 'address_2': new_address_2, 'tx_id': tx_cw}
        print_log("bitcoin_split_step GENERAL steps:" + str(const_params['steps']) + " " + str(split_tx), 'DEBUG', 1)
        if step + 1 == const_params['steps']:
            my_constant.BITCOIN_CW_AMOUNT_MUTEX.acquire()
            cw_tx = transfer_cw(primary_address, primary_tx)
            my_constant.BITCOIN_CW_AMOUNT_MUTEX.release()
            mongo.update_btc_tx_split(primary_address, primary_tx, const_params['steps'], split_tx, cw_tx)
        else:
            mongo.update_btc_tx_split(primary_address, primary_tx, const_params['steps'], split_tx)
            const_params['step'] += 1
            thread_1 = threading.Thread(target=bitcoin_split_step, args=(primary_address, primary_tx, new_address_1, tx_cw, amount_1, const_params, const_params['step']))
            thread_1.start()
            const_params['step'] += 1
            thread_2 = threading.Thread(target=bitcoin_split_step, args=(primary_address, primary_tx, new_address_2, tx_cw, amount_2, const_params, const_params['step']))
            thread_2.start()
        const_params['mutex'].release()
        my_constant.BTC_OUTPUTS_MUTEX.acquire()
        if lock_value in my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY]:
            my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY].remove(lock_value)
        my_constant.BTC_OUTPUTS_MUTEX.release()
    except Exception as e:
        if const_params['mutex'].locked():
            const_params['mutex'].release()
        my_constant.BTC_OUTPUTS_MUTEX.acquire()
        if lock_value in my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY]:
            my_constant.BTC_LOCKED_BALANCE[my_constant.BTC_SPLITTING_KEY].remove(lock_value)
        my_constant.BTC_OUTPUTS_MUTEX.release()
        print_log("bitcoin_split_step:" + str(e), "ERROR", 3)


def transfer_reverse_cw(order, order_tx, received):
    result = {'amount': 0, 'address': '', 'tx_id': ''}
    try:
        if received < get_btc_reverse_min(order['source_coin'], order['ex_price']):
            cw_amount = order_tx['dst_amt']
        else:
            cw_amount = received * order['ex_price'] * (EXCHANGE_FEE_DICT[order['source_coin']] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 2 / 100
        if cw_amount < 0:
            print_log("transfer_reverse_cw: cw_amount is less than zero", "ERROR", 3)
            return result
        my_constant.BITCOIN_CW_AMOUNT_MUTEX.acquire()
        my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] += cw_amount
        my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] = floating(my_constant.BITCOIN_CW_AMOUNT_FEE['amount'], 8)
        my_constant.BITCOIN_CW_AMOUNT_FEE['miner_fee'] = order['btc_miner_fee']
        result = transfer_cw(order['address'], order_tx['tx_id'])
        print_log('transfer_reverse_cw: result:' + str(result), 'ALARM', 5)
        my_constant.BITCOIN_CW_AMOUNT_MUTEX.release()
        return result
    except Exception as e:
        print_log("transfer_reverse_cw:" + str(e), "ERROR", 3)
        return result


def transfer_cw(order_address, order_tx_id):
    result = {'amount': 0, 'address': '', 'tx_id': ''}
    try:
        result['amount'] = my_constant.BITCOIN_CW_AMOUNT_FEE['amount']
        if BTC_IS_CW_TRANSFER_TO_BLOCKCHAIN is False:
            return result
        if BTC_CW_MINIMUM_AMOUNT > my_constant.BITCOIN_CW_AMOUNT_FEE['amount']:
            print_log("transfer_cw: sum of cw is less than minimum size", "NORMAL", 3)
            return result
        locked = lock_outputs(my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] + my_constant.BITCOIN_CW_AMOUNT_FEE['miner_fee'], 'CW:' + order_tx_id + ":" + order_address)
        if locked is None:
            print_log("transfer_cw: locked is None", "ERROR", 3)
            return result
        new_address = my_blockchain.get_new_address_from_blockchain()  # cw address replace need
        if new_address == '':
            print_log("transfer_cw: new address failed", "ERROR", 3)
            return result
        tx_hash = transfer_bitcoin(order_address, new_address, my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] + my_constant.BITCOIN_CW_AMOUNT_FEE['miner_fee'],
                                   locked, my_constant.BITCOIN_CW_AMOUNT_FEE['miner_fee'])
        if tx_hash is not None:
            result['tx_id'] = tx_hash
            result['amount'] = my_constant.BITCOIN_CW_AMOUNT_FEE['amount']
            result['address'] = new_address
            my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] = 0
        unlock_all_outputs(locked, 'CW:' + order_tx_id + ":" + order_address)
        return tx_hash
    except Exception as e:
        print_log("transfer_cw:" + str(e), "ERROR", 3)
        return result


def bitcoin_split_process(address, tx_id, amount, cw_fee, miner_fee):
    steps = get_split_count(amount)
    cw_amount = amount * cw_fee / 100
    if cw_amount > 0:
        my_constant.BITCOIN_CW_AMOUNT_MUTEX.acquire()
        my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] += cw_amount
        my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] = floating(my_constant.BITCOIN_CW_AMOUNT_FEE['amount'], 8)
        my_constant.BITCOIN_CW_AMOUNT_FEE['miner_fee'] = miner_fee
        my_constant.BITCOIN_CW_AMOUNT_MUTEX.release()
    if steps == 0:
        my_constant.BITCOIN_CW_AMOUNT_MUTEX.acquire()
        cw_tx = transfer_cw(address, tx_id)
        my_constant.BITCOIN_CW_AMOUNT_MUTEX.release()
        return cw_tx
    const_params = {'steps': steps, 'mutex': threading.Lock(), 'step': 0, 'miner_fee': miner_fee}
    thread = threading.Thread(target=bitcoin_split_step, args=(address, tx_id, address, tx_id, amount, const_params, const_params['step']))
    thread.start()
    return None


def get_split_count(amount):
    if amount >= 1:
        steps = BTC_SPLIT_COUNT_BY_1_AMOUNT
    elif amount >= 0.1:
        steps = BTC_SPLIT_COUNT_BY_01_AMOUNT
    elif amount >= 0.01:
        steps = BTC_SPLIT_COUNT_BY_001_AMOUNT
    else:
        steps = BTC_SPLIT_COUNT_BY_0001_AMOUNT
    return steps


def get_dest_amount(src_amount, ex_price, btc_miner_fee, cw_fee, decimal, token='', gas_price=0):
    splitting = get_split_count(src_amount)
    cw_amount = src_amount * cw_fee / 100
    my_constant.BITCOIN_CW_AMOUNT_MUTEX.acquire()
    btc_cw_miner_fee = 0
    if my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] < BTC_CW_MINIMUM_AMOUNT <= my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] + cw_amount:
        btc_cw_miner_fee = btc_miner_fee
    my_constant.BITCOIN_CW_AMOUNT_MUTEX.release()

    if token == '':
        dest = floating((src_amount - splitting * btc_miner_fee - src_amount * cw_fee / 100 - btc_cw_miner_fee) * ex_price,
            decimal)
    else:
        if token == 'ETH':
            dest = floating((src_amount - splitting * btc_miner_fee - src_amount * cw_fee / 100 - btc_cw_miner_fee) * ex_price,
                decimal)
            dest -= my_ethereum.get_eth_limit(gas_price, ETH_GAS_MIN_LIMIT)
        else:
            gas_reduce = my_ethereum.get_eth_limit(gas_price) * my_ethereum.get_btc_ex_reverse('ETH')
            dest = floating((src_amount - splitting * btc_miner_fee - src_amount * cw_fee / 100 - btc_cw_miner_fee - gas_reduce) * ex_price,
                decimal)
    return dest


def check_conf(order, order_tx):
    step = order_tx['step']
    if step >= 8 or order['src_amt'] == 0:
        print_log(order['address'] + ":" + order_tx['tx_id'] + " It is completed order. Thread is returned.")
        return
    my_constant.CHECK_THREAD_MUTEX.acquire()
    if check_thread_order(order_tx['tx_id'] + ":" + order['address']):
        my_constant.CHECK_THREAD_MUTEX.release()
        print_log(order['address'] + ":" + order_tx['tx_id']
                  + " This transaction is already working on progress. Thread is returned.")
        return
    my_constant.THREAD_ORDERS.append(order_tx['tx_id'] + ":" + order['address'])
    my_constant.CHECK_THREAD_MUTEX.release()
    print_log('thread started: ' + order_tx['tx_id'], "DEBUG", 3)
    dst_amt = get_reverse_dest_amount(order_tx['amount'], order['ex_price'],
                                      (EXCHANGE_FEE_DICT[order['source_coin']] + EXCHANGE_EXTRA_FEE_FOR_REVERSE) / 2,
                                      my_constant.BITCOIN_DECIMALS, order['gas_reduce'], order['btc_miner_fee'])
    order_tx['dst_amt'] = dst_amt
    mongo.update_tx(order['address'], order_tx)
    dst_amt -= order_tx['sent']
    refund_amt = order_tx['refunded']
    priority = order_tx['miner_fee']
    status = get_order_status(order)
    if status == 'created' and step == 0 and dst_amt > 0:
        if priority[:3] != 'low' or DEBUG_MODE_TO_RESERVE:
            locked = lock_outputs(dst_amt, order_tx['tx_id'] + ":" + order['address'])
            if locked is None:
                if is_enough_balance():
                    ret = mongo.update_transaction_status(order['address'], order_tx, "created", "queued")
                else:
                    ret = mongo.update_transaction_status(order['address'], order_tx, "created", "exceed")
            else:
                ret = mongo.update_transaction_status(order['address'], order_tx, "created", "reserve")
            if ret < 0:
                exit_thread(order['address'], locked, order_tx)
                return
        else:
            locked = None
            ret = mongo.update_transaction_status(order['address'], order_tx, "created", "low")
            if ret < 0:
                exit_thread(order['address'], locked, order_tx)
                return
    else:
        locked = None
    received = order_tx['received']
    while True:
        try:
            if received <= 0.0:
                # print_log('strange begin debug order_tx:' + str(order_tx), 'DEBUG', 3)
                result = crypto_confirm_payment(order, order_tx)
                if result is not None:
                    if order['source_coin'] == 'XMR':
                        received = result
                        order_tx['received'] = received
                    elif order['source_coin'] == 'XVG':
                        received = result
                        order_tx['received'] = received
                    elif is_key_dict(my_constant.ETH_TOKEN_LIST, order['source_coin']):
                        received = result['amount']
                        order_tx['received'] = received
                        order_tx['cold_index'] = result['cold_index']
                        order_tx['cold_token_tx'] = result['cold_token_tx']
                        order_tx['cold_eth_tx'] = result['cold_eth_tx']
            if received <= 0.0:
                print_log(order['address'] + ":" + order_tx['tx_id'] + " Order confirmation error. Thread is returned.")
                exit_thread(order['address'], locked, order_tx)
                return
            confirmed = mongo.update_transaction_status(order['address'], order_tx, "confirmed", '', True)
            if confirmed < 0:
                exit_thread(order['address'], locked, order_tx)
                return
            print_log(
                order['address'] + ":" + order_tx['tx_id'] + " Tx Confirmed. Amount:" + str(received)
                + " Confirmed Duration:" + str(confirmed) + "s", "NORMAL", 5)
            if received < get_btc_reverse_min(order['source_coin'], order['ex_price']):
                unlock_all_outputs(locked, order_tx['tx_id'] + ":" + order['address'])
                payment = 3
            else:
                if received == order['src_amt']:
                    payment = 0  # good payment
                elif received > order['src_amt']:
                    payment = 1  # over payment
                else:
                    payment = 2  # under payment
            if payment == 3:
                tx_cw = transfer_reverse_cw(order, order_tx, received)
                order_tx['tx_cw'] = tx_cw
                step = 4
                refund_amt = received
                mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
            elif step == 2:
                tx_cw = transfer_reverse_cw(order, order_tx, received)
                order_tx['tx_cw'] = tx_cw
                step = 4
                mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
                print_log(order['address'] + ":" + order_tx['tx_id'] + " Tx Completed. Amount:" + str(order['src_amt'])
                          + " Received Amount:" + str(received), "NORMAL", 5)
                for item in order_tx['tx']:
                    print_log(order['address'] + ":" + order_tx['tx_id'] + " ==tx:" + str(item), "NORMAL", 5)
                print_log(order['address'] + ":" + order_tx['tx_id'] + " tx_cw:" + str(order_tx['tx_cw']), "NORMAL", 5)
                ret = mongo.update_transaction_status(order['address'], order_tx, "completed", "Tx completed.")
                if ret < 0:
                    exit_thread(order['address'], locked, order_tx)
                    return
            elif step == 0:
                if status == 'created':
                    if dst_amt > 0:
                        result = transfer_to_address(order['address'], order['dst_address'], dst_amt, order_tx, refund_amt,
                                                     received, order_tx['dest_miner_fee'], locked, step)
                        step = result['step']
                        refund_amt = result['refund_amt']
                    else:
                        step = 2
                    if step == 2:
                        tx_cw = transfer_reverse_cw(order, order_tx, received)
                        order_tx['tx_cw'] = tx_cw
                        step = 4
                        mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
                        print_log(
                            order['address'] + ":" + order_tx['tx_id'] + " Tx Completed. Amount:" + str(order['src_amt'])
                            + " Received Amount:" + str(received), "NORMAL", 5)
                        for item in order_tx['tx']:
                            print_log(order['address'] + ":" + order_tx['tx_id'] + " ==tx:" + str(item), "NORMAL", 5)
                        print_log(order['address'] + ":" + order_tx['tx_id'] + " tx_cw:" + str(order_tx['tx_cw']), "NORMAL",
                                  5)
                        ret = mongo.update_transaction_status(order['address'], order_tx, "completed", "Tx completed.")
                        if ret < 0:
                            exit_thread(order['address'], locked, order_tx)
                            return
                else:
                    refund_amt = received
                    step = 5
                    mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
            refund_amt = float(refund_amt)
            if refund_amt > 0 and step <= 5:
                detail = ""
                while True:
                    try:
                        if payment == 3:
                            detail = "Received underpayment with below LIMIT_MIN_BTC."
                            step = 6
                            refund_amt = 0
                            mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
                            ret = mongo.update_transaction_status(order['address'], order_tx, "deposit", detail)
                            if ret < 0:
                                exit_thread(order['address'], locked, order_tx)
                                return
                            break
                        else:
                            tx_ref = reverse_refund_payment(order, order_tx, refund_amt)
                            if tx_ref is None:
                                print_log("check_conf transfer_refund_payment tx_ref is None " + "input_tx: " +
                                          order_tx['tx_id'] + " dest: " + order['refund_address'] + " amount: " + str(
                                    refund_amt),
                                          "ERROR", 5)
                                time.sleep(5)
                                continue
                            order_tx['tx_ref'] = tx_ref
                            try:
                                if step == 5:
                                    order = mongo.find_order_by_address(order['address'])
                                    refund = float(order['refunded_amt']) + refund_amt
                                    mongo.update_refunded_amount(order['address'], refund)
                                    detail = "Order was already timed out or completed."
                                elif step == 3:
                                    detail = "Invalid BTC address."
                                elif step == 4:
                                    detail = "Partial refunded, because not enough balance in our end."
                                else:
                                    detail = "Not enough balance in our end."
                                step = 7
                                mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
                                unlock_all_outputs(locked, order_tx['tx_id'] + ":" + order['address'])
                                ret = mongo.update_transaction_status(order['address'], order_tx, "refunded", detail)
                                if ret < 0:
                                    exit_thread(order['address'], locked, order_tx)
                                    return
                            except Exception as e:
                                print_log("4 DB updating error: " + str(e), "ERROR", 5)
                                detail = "DB updating error, but refunded completely"
                            break
                    except Exception as err:
                        print_log("4 " + str(err) + " Refund Address:" +
                                  order['refund_address'] + " Refund Amount:" + str(refund_amt), "ERROR", 5)
                        time.sleep(5)
                print_log(order['address'] + ":" + order_tx['tx_id'] + " " + detail, "NORMAL", 5)
                for item in order_tx['tx']:
                    print_log(order['address'] + ":" + order_tx['tx_id'] + " ==tx:" + str(item), "NORMAL", 5)
                print_log(order['address'] + ":" + order_tx['tx_id'] + " tx_cw:" + str(order_tx['tx_cw']), "NORMAL", 5)
                print_log(order['address'] + ":" + order_tx['tx_id'] + " tx_ref:" + str(order_tx['tx_ref']), "NORMAL", 5)
            step = 8
            mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
            unlock_all_outputs(locked, order_tx['tx_id'] + ":" + order['address'])
            break
        except Exception as err:
            print_log("5 " + str(err), "ERROR", 5)
            time.sleep(10)
    my_constant.CHECK_THREAD_MUTEX.acquire()
    my_constant.THREAD_ORDERS.remove(order_tx['tx_id'] + ":" + order['address'])
    my_constant.CHECK_THREAD_MUTEX.release()


def get_reverse_dest_amount(amount, ex_price, cw_fee, decimal, gas_reduce=0, btc_miner_fee=0):
    cw_amount = amount * ex_price * cw_fee / 100
    my_constant.BITCOIN_CW_AMOUNT_MUTEX.acquire()
    btc_cw_miner_fee = 0
    if my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] < BTC_CW_MINIMUM_AMOUNT <= my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] + cw_amount:
        btc_cw_miner_fee = btc_miner_fee
    my_constant.BITCOIN_CW_AMOUNT_MUTEX.release()
    return floating(amount * ex_price * (1 - cw_fee / 100) - gas_reduce - btc_cw_miner_fee, decimal)


def is_reserve_tx_from_key(key, value):
    try:
        if value in my_constant.BTC_LOCKED_BALANCE[key]:
            return True
        return False
    except Exception as e:
        print_log("is_reserve_tx_from_key:" + str(e), "ERROR", 3)
        return False


def is_reserve_tx(value):
    try:
        for locked_list in list(my_constant.BTC_LOCKED_BALANCE.values()):
            if value in locked_list:
                return True
        return False
    except Exception as e:
        print_log("is_reserve_tx:" + str(e), "ERROR", 3)
        return False


def get_dest_miner_fee(amount, ex_price, btc_miner_fee=0):
    if btc_miner_fee == 0:
        btc_miner_fee = calc_miner_fee()
    dest = float(amount) * ex_price
    locked = find_proper_account(dest)
    if locked is None or len(locked) <= 2:
        return btc_miner_fee
    return (len(locked) - 1) * btc_miner_fee


def sort_list_tx(tx_lists):
    try:
        for i in range(0, len(tx_lists)):
            for j in range(i + 1, len(tx_lists)):
                if tx_lists[i]['amount'] < tx_lists[j]['amount']:
                    temp = tx_lists[i]
                    tx_lists[i] = tx_lists[j]
                    tx_lists[j] = temp
    except Exception as e:
        print_log('sort_list_tx:' + str(e), 'ERROR', 3)
    return tx_lists


def find_proper_account(dst_amt):
    while True:
        try:
            locked = []
            btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
            lists = btc_rpc_connection.listunspent(1, my_constant.BITCOIN_MAXIMUM_UNUSED_CONFIRM, [])
            lists = sort_list_tx(lists)
            amount = 0
            for item in lists:
                lock_value = {'address': str(item['address']), 'tx_id': str(item['txid']), 'amount': float(item['amount'])}
                if is_reserve_tx(lock_value):
                    continue
                amount += float(item['amount'])
                locked.append(lock_value)
                if amount >= dst_amt:
                    return locked
            break
        except Exception as e:
            print_log("btc:find_proper_account:" + str(e), "ERROR", 3)
            time.sleep(1)
    return None


def lock_outputs(dst_amt, ident_address):
    try:
        my_constant.BTC_OUTPUTS_MUTEX.acquire()
        locked = find_proper_account(dst_amt)
        if locked is not None:
            if is_key_dict(my_constant.BTC_LOCKED_BALANCE, ident_address) is False:
                my_constant.BTC_LOCKED_BALANCE[ident_address] = []
            for item in locked:
                if is_reserve_tx_from_key(ident_address, item) is True:
                    continue
                my_constant.BTC_LOCKED_BALANCE[ident_address].append(item)
        my_constant.BTC_OUTPUTS_MUTEX.release()
        return locked
    except Exception as e:
        if my_constant.BTC_OUTPUTS_MUTEX.locked():
            my_constant.BTC_OUTPUTS_MUTEX.release()
        print_log("btc:lock_outputs:" + str(e), "ERROR", 3)
        return None


def remove_locked_tx(key, value):
    if key in my_constant.BTC_LOCKED_BALANCE:
        if value in my_constant.BTC_LOCKED_BALANCE[key]:
            my_constant.BTC_LOCKED_BALANCE[key].remove(value)


def exit_thread(address, locked, order_tx):
    if locked is not None:
        try:
            my_constant.BTC_OUTPUTS_MUTEX.acquire()
            if order_tx['tx_id'] + ":" + address in my_constant.BTC_LOCKED_BALANCE:
                my_constant.BTC_LOCKED_BALANCE.pop(order_tx['tx_id'] + ":" + address)
            my_constant.BTC_OUTPUTS_MUTEX.release()
        except Exception as e:
            if my_constant.BTC_OUTPUTS_MUTEX.locked():
                my_constant.BTC_OUTPUTS_MUTEX.release()
            print_log("exit_thread: " + str(e), "WARNING", 3)
    my_constant.CHECK_THREAD_MUTEX.acquire()
    if order_tx['tx_id'] + ":" + address in my_constant.THREAD_ORDERS:
        my_constant.THREAD_ORDERS.remove(order_tx['tx_id'] + ":" + address)
    my_constant.CHECK_THREAD_MUTEX.release()


def unlock_all_outputs(locked, ident_address):
    if locked is not None:
        try:
            my_constant.BTC_OUTPUTS_MUTEX.acquire()
            if ident_address in my_constant.BTC_LOCKED_BALANCE:
                my_constant.BTC_LOCKED_BALANCE.pop(ident_address)
            my_constant.BTC_OUTPUTS_MUTEX.release()
        except Exception as e:
            if my_constant.BTC_OUTPUTS_MUTEX.locked():
                my_constant.BTC_OUTPUTS_MUTEX.release()
            print_log("btc:unlock_all_outputs: " + str(e), "WARNING", 3)


def transfer_bitcoin(address, dst_address, dst_amount, locked, miner_fee):
    if locked is None:
        print_log("btc:transfer_bitcoin: locked is none address:" + address + " dest_address:" + dst_address, "WARNING", 3)
        return None
    inputs = []
    first_amount = floating(dst_amount - miner_fee, 8)
    outputs = [{dst_address: first_amount}]
    amount = 0
    for item in locked:
        vout = get_vout_from_unspend_tx(item['address'], item['tx_id'])
        if vout < 0:
            print_log("btc:transfer_bitcoin: already spent address:" + address + " dest_address:" + dst_address, "WARNING", 3)
            return None
        inputs.append({'txid': item['tx_id'], 'vout': vout})
        amount += item['amount']
    if amount < dst_amount:
        print_log("btc:transfer_bitcoin: exceed amount address:" + address + " dest_address:" + dst_address, "WARNING", 3)
        return None
    balance = floating(amount - first_amount - miner_fee, 8)
    if balance > 0:
        new_address = get_btc_new_address()
        outputs.append({new_address: balance})
    tx_hash = send_btc(inputs, outputs, miner_fee)
    if tx_hash is None:
        print_log("btc:transfer_bitcoin: transfer failed address:" + address + " dest_address:" + dst_address, "WARNING", 3)
        return None
    return tx_hash


def partial_transfer(address, dst_address, dst_amt, locked, order_tx, miner_fee):
    tx_hash = transfer_bitcoin(address, dst_address, dst_amt, locked, miner_fee)
    if tx_hash is None:
        return 1
    order_tx['tx'].append(tx_hash)
    order_tx['sent'] += dst_amt
    mongo.update_tx(address, order_tx)
    return 0


def transfer_to_address(address, dst_address, dst_amt, order_tx, refund_amt, received, miner_fee, locked=None, step=0):
    result = {'step': step, 'refund_amt': refund_amt}
    if step != 0:
        unlock_all_outputs(locked, order_tx['tx_id'] + ":" + address)
        return result
    try:
        while locked is None:
            locked = lock_outputs(dst_amt, order_tx['tx_id'] + ":" + address)
            if is_enough_balance() is False:
                break
            time.sleep(10)
            print_log('btc:transfer_to_address our balance was locked', 'WARNING', 3)
            # mongo.update_transaction_status(address, order_tx, order_tx['status'], 'queued')
        if locked is None:
            refund_amt = received
            step = 1
            mongo.update_tx_step(address, order_tx, step, refund_amt)
            result = {'step': step, 'refund_amt': refund_amt}
        else:
            step = partial_transfer(address, dst_address, dst_amt, locked, order_tx, miner_fee)
            if step != 0:
                refund_amt = received
            else:
                refund_amt = 0
                step = 2
            mongo.update_tx_step(address, order_tx, step, refund_amt)
            result = {'step': step, 'refund_amt': refund_amt}
        unlock_all_outputs(locked, order_tx['tx_id'] + ":" + address)
        return result
    except Exception as e:
        print_log("bitcoin:transfer_to_address:" + str(e), "ERROR", 3)
        unlock_all_outputs(locked, order_tx['tx_id'] + ":" + address)
        return result


def reverse_refund_payment(order, order_tx, refund_amount):
    try:
        tx_ref = []
        if order['source_coin'] == 'XMR':
            my_monero.refund_payment(order['address'], order['refund_address'], refund_amount, order_tx, tx_ref)
        elif order['source_coin'] == 'XVG':
            tx_ref = my_verge.refund_payment(order['address'], order['refund_address'], refund_amount, order_tx)
        elif is_key_dict(my_constant.ETH_TOKEN_LIST, order['source_coin']) is True:
            my_ethereum.refund_payment(order['address'], order['refund_address'], refund_amount, order_tx, order['source_coin'], order['gas_price'], tx_ref)
        return tx_ref
    except Exception as e:
        print_log('reverse_refund_payment:' + str(e), "ERROR", 3)
        return None


def refund_payment(address, dst_address, dst_amount, order_tx, miner_fee=0):
    locked = None
    try:
        while locked is None:
            locked = lock_outputs(dst_amount, 'ref_' + order_tx['tx_id'] + ":" + address)
            if is_enough_balance(dst_amount) is False:
                break
            time.sleep(10)
            print_log('btc:refund_payment our balance was locked', 'WARNING', 3)
        if locked is None:
            print_log("btc:refund_payment: locked is none address:" + address + " dest_address:" + dst_address,
                      "WARNING", 3)
            return None
        dest_miner_fee = get_dest_miner_fee(dst_amount, 1, miner_fee)
        if dest_miner_fee >= dst_amount:
            dest_miner_fee = miner_fee
        inputs = []
        first_amount = floating(dst_amount - dest_miner_fee, get_btc_decimals())
        outputs = [{dst_address: first_amount}]
        amount = 0
        for item in locked:
            vout = get_vout_from_unspend_tx(item['address'], item['tx_id'])
            if vout < 0:
                print_log("btc:refund_payment: already spent address:" + address + " dest_address:" + dst_address,
                          "WARNING", 3)
                unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
                return None
            inputs.append({'txid': item['tx_id'], 'vout': vout})
            amount += item['amount']
        if amount < dst_amount:
            print_log("btc:refund_payment: exceed amount address:" + address + " dest_address:" + dst_address,
                      "WARNING", 3)
            unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
            return None
        balance = floating(amount - first_amount - dest_miner_fee, get_btc_decimals())
        if balance > 0:
            new_address = get_btc_new_address()
            outputs.append({new_address: balance})
        tx_hash = send_btc(inputs, outputs, dest_miner_fee)
        if tx_hash is None:
            print_log("btc:refund_payment: transfer failed address:" + address + " dest_address:" + dst_address,
                      "WARNING", 3)
            unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
            return None
        result = [tx_hash]
        unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
        return result
    except Exception as e:
        print_log('btc:refund_payment:' + str(e), "ERROR", 3)
        unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
        return None


def update_order_amounts(order, btc_lists):
    try:
        received = 0.0
        refunded = 0.0
        dst_amt = 0.0
        completed = True
        received_usd = 0.0
        for item in order['out_tx_list']:
            if item['step'] == 0 and find_item_in_btc_list(item, btc_lists) is False:
                mongo.update_bad_tx(item['address'], item)
                continue
            if item['amount'] < get_btc_min(order['dest_coin']):
                continue
            received += item['amount']
            refunded += item['refunded']
            dst_amt += (item['dst_amt'] - item['dest_miner_fee'])
            if item['status'] == 'completed':
                received_usd += item['amount'] * order['btc_usd']
            if item['step'] != 8:
                completed = False
        mongo.update_order_amount(order['address'], order['src_amt'], floating(dst_amt, 3),
                                  floating(received, 8), floating(refunded, 8), floating(received_usd, 2))
        if received == 0:
            return 1  # cancelling
        if completed is True:
            return 0
        return -1
    except Exception as e:
        print_log("update_order_amounts:" + str(e), "ERROR", 3)
        return -2


def find_item_in_btc_list(item, btc_lists):
    found = False
    try:
        for btc in btc_lists:
            if btc['txid'] != item['tx_id']:
                continue
            amount = get_amount_from_tx(item['address'], btc)
            if amount <= 0:
                continue
            found = True
    except Exception as e:
        print_log("find_item_in_btc_list:" + str(e), "ERROR", 3)
    return found


def is_enough_balance(self_balance=0):
    try:
        orders = mongo.pull_by_status('created', dest_coin='BTC')
        amount = 0
        for order in orders:
            for item in order['out_tx_list']:
                if item['step'] >= 2:
                    continue
                amount += item['dst_amt']
        amount += my_constant.BITCOIN_CW_AMOUNT_FEE['amount']
        amount += self_balance
        btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
        balance = btc_rpc_connection.getbalance()
        print_log("is_enough_balance: balance:" + str(balance) + ", amount:" + str(amount), "ALARM", 5)
        if balance > amount:
            return True
        return False
    except Exception as e:
        print_log("is_enough_balance:" + str(e), "ERROR", 3)
        return False


def load_balancing():
    while True:
        try:
            time.sleep(my_constant.BTC_LOAD_BALANCING_PERIOD)
            my_constant.BTC_OUTPUTS_MUTEX.acquire()
            btc_rpc_connection = AuthServiceProxy(my_constant.RPC_BTC_URL, timeout=5000)
            lists = btc_rpc_connection.listunspent(my_constant.BTC_MIN_LOAD_BALANCING_CONFIRM, my_constant.BITCOIN_MAXIMUM_UNUSED_CONFIRM, [])
            if len(lists) <= 0:
                my_constant.BTC_OUTPUTS_MUTEX.release()
                continue
            max_tx = lists[0]
            for item in lists:
                lock_value = {'address': str(item['address']), 'tx_id': str(item['txid']), 'amount': float(item['amount'])}
                if is_reserve_tx(lock_value):
                    continue
                if max_tx['amount'] < item['amount']:
                    max_tx = item
            if float(max_tx['amount']) <= BTC_MIN_LOAD_BALANCING_AMOUNT:
                my_constant.BTC_OUTPUTS_MUTEX.release()
                print_log("btc:load_balancing: not enough tx 0", "ERROR", 3)
                continue
            lock_value = {'address': str(max_tx['address']), 'tx_id': str(max_tx['txid']), 'amount': float(max_tx['amount'])}
            ident_address = 'LOAD_BALANCING'
            if is_key_dict(my_constant.BTC_LOCKED_BALANCE, ident_address) is False:
                my_constant.BTC_LOCKED_BALANCE[ident_address] = []
            if is_reserve_tx_from_key(ident_address, lock_value) is True:
                my_constant.BTC_OUTPUTS_MUTEX.release()
                print_log("btc:load_balancing: reserved tx", "ERROR", 3)
                continue
            counts = float(max_tx['amount']) // BTC_MIN_LOAD_BALANCING_AMOUNT + 1
            if counts < 2:
                my_constant.BTC_OUTPUTS_MUTEX.release()
                print_log("btc:load_balancing: not enough tx 1", "ERROR", 3)
                continue
            btc_miner_fee = floating(calc_miner_fee() * (counts - 1), 8)
            if float(max_tx['amount']) - btc_miner_fee <= BTC_MIN_LOAD_BALANCING_AMOUNT:
                my_constant.BTC_OUTPUTS_MUTEX.release()
                print_log("btc:load_balancing: not enough tx 2", "ERROR", 3)
                continue
            vout = get_vout_from_unspend_tx(str(max_tx['address']), str(max_tx['txid']))
            if vout < 0:
                my_constant.BTC_OUTPUTS_MUTEX.release()
                print_log("btc:load_balancing: already spent tx", "ERROR", 3)
                continue
            my_constant.BTC_LOCKED_BALANCE[ident_address].append(lock_value)
            my_constant.BTC_OUTPUTS_MUTEX.release()
            try:
                inputs = [{'txid': str(max_tx['txid']), 'vout': vout}]
                total_amount = float(max_tx['amount']) - btc_miner_fee
                amount = 0
                outputs = []
                while True:
                    amount += BTC_MIN_LOAD_BALANCING_AMOUNT
                    if amount >= total_amount:
                        rest_amount = floating(total_amount - (amount - BTC_MIN_LOAD_BALANCING_AMOUNT), get_btc_decimals())
                        new_address = get_btc_new_address()
                        outputs.append({new_address: rest_amount})
                        break
                    new_address = get_btc_new_address()
                    outputs.append({new_address:BTC_MIN_LOAD_BALANCING_AMOUNT})
                tx_cw = send_btc(inputs, outputs, btc_miner_fee)
                print_log("btc:load_balancing: finished tx_cw:" + str(tx_cw), "ALARM", 3)
                unlock_all_outputs([lock_value], ident_address)
            except Exception as e:
                print_log("btc:load_balancing: inner:" + str(e), "ERROR", 3)
        except Exception as e:
            print_log("btc:load_balancing:" + str(e), "ERROR", 3)
            if my_constant.BTC_OUTPUTS_MUTEX.locked():
                my_constant.BTC_OUTPUTS_MUTEX.release()
