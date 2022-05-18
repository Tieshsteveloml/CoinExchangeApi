import threading
import time
from datetime import datetime, timedelta

import mongodb as mongo
from config.config import *
from currencies.exchange import get_btc_xvg_exchange
from currencies import bitcoin as my_bitcoin
from general import *
import vergerpc


def get_connection():
    max_tries = 10
    tried = 0
    while tried < max_tries:
        try:
            conn = vergerpc.connect_to_remote(XVG_RPC_USER, XVG_RPC_PASSWORD, host=XVG_RPC_HOST, port=XVG_RPC_PORT)
            return conn
        except Exception as e:
            print_log('get_connection:' + str(e), "ERROR", 3)
            time.sleep(1)
    return None


def wallet_total_balance():
    try:
        orders = mongo.pull_by_status('created', dest_coin='XVG')
        amount = 0
        for order in orders:
            for item in order['out_tx_list']:
                if item['step'] >= 2:
                    continue
                amount += item['dst_amt']
        conn = get_connection()
        balance = conn.getbalance()
        amount = float(balance) - amount
        return amount
    except Exception as e:
        print_log("wallet_total_balance:" + str(e), "ERROR", 3)
        return 0


def get_btc_ex_price_from_url():
    try:
        prices = get_btc_xvg_exchange()
        if prices is None:
            return my_constant.EX_BTC_XVG_PRICE
        my_constant.EX_BTC_XVG_PRICE = prices
    except Exception as err:
        print_log("verge:get_btc_ex_price_from_url:" + str(err), "ERROR", 3)
    return my_constant.EX_BTC_XVG_PRICE


def get_ex_prices_thread():
    try:
        get_btc_ex_price_from_url()
        ex_price = get_btc_ex_price()
        max_btc = get_btc_max(ex_price)
        my_constant.XVG_MAX_BTC_BALANCE = max_btc
    except Exception as e:
        print_log("verge:get_ex_prices_thread:" + str(e), "ERROR", 3)


def get_btc_ex_price():
    while True:
        try:
            if my_constant.EX_BTC_XVG_PRICE['ex_price'] == 0.0:
                my_constant.EX_BTC_XVG_PRICE = get_btc_ex_price_from_url()
            return my_constant.EX_BTC_XVG_PRICE['ex_price']
        except Exception as e:
            print_log("verge:get_btc_ex_price:" + str(e), "ERROR", 3)


def get_btc_ex_reverse():
    while True:
        try:
            if my_constant.EX_BTC_XVG_PRICE['ex_reverse'] == 0.0:
                my_constant.EX_BTC_XVG_PRICE = get_btc_ex_price_from_url()
            return my_constant.EX_BTC_XVG_PRICE['ex_reverse']
        except Exception as e:
            print_log("verge:get_btc_ex_reverse:" + str(e), "ERROR", 3)


def get_btc_max(ex_price):
    if MULTI_OUTPUTS:
        max_bal = floating(wallet_total_balance() / ex_price, get_decimals(True))
    else:
        max_bal = floating(wallet_total_balance() / ex_price, get_decimals(True))
    return max_bal


def get_btc_max_balance():
    return my_constant.XVG_MAX_BTC_BALANCE


def get_decimals(reverse=False):
    if reverse:
        return my_constant.VERGE_DECIMALS['reverse']
    else:
        return my_constant.VERGE_DECIMALS['forward']


def get_xvg_new_address():
    while True:
        try:
            conn = get_connection()
            address = conn.getnewaddress()
            break
        except Exception as err:
            print_log("get_xvg_new_address:" + str(err), "ERROR", 3)
            pass
    return address


def calc_miner_fee(block_size=XVG_MINER_FEE_BLOCK_SIZE, level=XVG_MINER_FEE_CONFIRMATION_TARGET):
    while True:
        try:
            return 0.1
            '''
            conn = get_connection()
            fee_per_k_byte = conn.estimatesmartfee(level, XVG_MINER_FEE_ESTIMATE_MODE)
            fee = float(fee_per_k_byte['feerate']) / 1000 * block_size
            if fee < 0.1:
                fee = 0.1
            print_log("xvg:calc_miner_fee size: " + str(block_size) + " fee: " + str(fee), "NORMAL", 5)
            return floating(fee, 1)
            '''
        except Exception as err:
            print_log("xvg:calc_miner_fee:" + str(err), "ERROR", 3)
            time.sleep(1)


def get_dest_miner_fee(amount, xvg_miner_fee=0):
    if xvg_miner_fee == 0:
        xvg_miner_fee = calc_miner_fee()
    return xvg_miner_fee
    '''
    locked = find_proper_account(float(amount))
    if locked is None or len(locked) <= 2:
        return xvg_miner_fee
    return (len(locked) - 1) * xvg_miner_fee
    '''


def sort_list_tx(tx_lists):
    try:
        for i in range(0, len(tx_lists)):
            for j in range(i + 1, len(tx_lists)):
                if tx_lists[i].amount < tx_lists[j].amount:
                    temp = tx_lists[i]
                    tx_lists[i] = tx_lists[j]
                    tx_lists[j] = temp
    except Exception as e:
        print_log('xvg:sort_list_tx:' + str(e), 'ERROR', 3)
    return tx_lists


def find_proper_account(dst_amt):
    while True:
        try:
            locked = []
            conn = get_connection()
            lists = conn.listunspent(1, my_constant.VERGE_MAXIMUM_UNUSED_CONFIRM)
            amount = 0
            lists = sort_list_tx(lists)
            for item in lists:
                lock_value = {'address': item.address, 'tx_id': item.txid, 'amount': float(item.amount)}
                if is_reserve_tx(lock_value):
                    continue
                amount += float(item.amount)
                locked.append(lock_value)
                if amount >= dst_amt:
                    return locked
            break
        except Exception as e:
            print_log("xvg:find_proper_account:" + str(e), "ERROR", 3)
            time.sleep(1)
    return None


def is_reserve_tx(value):
    try:
        for locked_list in list(my_constant.XVG_LOCKED_BALANCE.values()):
            if value in locked_list:
                return True
        return False
    except Exception as e:
        print_log("xvg:is_reserve_tx:" + str(e), "ERROR", 3)
        return False


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
    dst_amt = my_bitcoin.get_dest_amount(order_tx['amount'], order['ex_price'], order['btc_miner_fee'],
                                         EXCHANGE_FEE_DICT[order['dest_coin']] / 2,
                                         get_verge_decimals())
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
                received = my_bitcoin.confirm_payment(order, order_tx)
                order_tx['received'] = received
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
            if received < my_bitcoin.get_btc_min(order['dest_coin']):
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
                my_constant.BITCOIN_CW_AMOUNT_MUTEX.acquire()
                my_constant.BITCOIN_CW_AMOUNT_FEE['amount'] += received
                my_constant.BITCOIN_CW_AMOUNT_FEE['miner_fee'] = order['btc_miner_fee']
                my_constant.BITCOIN_CW_AMOUNT_MUTEX.release()
                step = 4
                refund_amt = received
                mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
            elif step == 2:
                amount = floating(received - refund_amt, 8)
                tx_cw = my_bitcoin.bitcoin_split_process(order['address'], order_tx['tx_id'], amount,
                                                         EXCHANGE_FEE_DICT[order['dest_coin']] / 2,
                                                         order['btc_miner_fee'])
                if tx_cw is not None:
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
                        amount = floating(received - refund_amt, 8)
                        tx_cw = my_bitcoin.bitcoin_split_process(order['address'], order_tx['tx_id'], amount,
                                                                 EXCHANGE_FEE_DICT[order['dest_coin']] / 2,
                                                                 order['btc_miner_fee'])
                        if tx_cw is not None:
                            order_tx['tx_cw'] = tx_cw
                        step = 4
                        mongo.update_tx_step(order['address'], order_tx, step, refund_amt)
                        print_log(order['address'] + ":" + order_tx['tx_id'] + " Tx Completed. Amount:" + str(
                            order['src_amt'])
                                  + " Received Amount:" + str(received), "NORMAL", 5)
                        for item in order_tx['tx']:
                            print_log(order['address'] + ":" + order_tx['tx_id'] + " ==tx:" + str(item), "NORMAL", 5)
                        print_log(order['address'] + ":" + order_tx['tx_id'] + " tx_cw:" + str(order_tx['tx_cw']),
                                  "NORMAL", 5)
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
                            tx_ref = my_bitcoin.refund_payment(order['address'], order['refund_address'], refund_amt, order_tx, order['btc_miner_fee'])
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
                                    detail = "Invalid XVG address."
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


def lock_outputs(dst_amt, ident_address):
    try:
        my_constant.XVG_OUTPUTS_MUTEX.acquire()
        locked = find_proper_account(dst_amt)
        if locked is not None:
            if is_key_dict(my_constant.XVG_LOCKED_BALANCE, ident_address) is False:
                my_constant.XVG_LOCKED_BALANCE[ident_address] = []
            for item in locked:
                if is_reserve_tx_from_key(ident_address, item) is True:
                    continue
                my_constant.XVG_LOCKED_BALANCE[ident_address].append(item)
        my_constant.XVG_OUTPUTS_MUTEX.release()
        return locked
    except Exception as e:
        if my_constant.XVG_OUTPUTS_MUTEX.locked():
            my_constant.XVG_OUTPUTS_MUTEX.release()
        print_log("xvg:lock_outputs:" + str(e), "ERROR", 3)
        return None


def is_reserve_tx_from_key(key, value):
    try:
        if value in my_constant.XVG_LOCKED_BALANCE[key]:
            return True
        return False
    except Exception as e:
        print_log("xvg:is_reserve_tx_from_key:" + str(e), "ERROR", 3)
        return False


def is_enough_balance(self_balance=0):
    try:
        orders = mongo.pull_by_status('created', dest_coin='XVG')
        amount = 0
        for order in orders:
            for item in order['out_tx_list']:
                if item['step'] >= 2:
                    continue
                amount += item['dst_amt']
        amount += self_balance
        conn = get_connection()
        balance = float(conn.getbalance())
        print_log("xvg:is_enough_balance: balance:" + str(balance) + ", amount:" + str(amount), "ALARM", 5)
        if balance > amount:
            return True
        return False
    except Exception as e:
        print_log("xvg:is_enough_balance:" + str(e), "ERROR", 3)
        return False


def exit_thread(address, locked, order_tx):
    if locked is not None:
        try:
            my_constant.XVG_OUTPUTS_MUTEX.acquire()
            if order_tx['tx_id'] + ":" + address in my_constant.XVG_LOCKED_BALANCE:
                my_constant.XVG_LOCKED_BALANCE.pop(order_tx['tx_id'] + ":" + address)
            my_constant.XVG_OUTPUTS_MUTEX.release()
        except Exception as e:
            if my_constant.XVG_OUTPUTS_MUTEX.locked():
                my_constant.XVG_OUTPUTS_MUTEX.release()
            print_log("xvg:exit_thread: " + str(e), "WARNING", 3)
    my_constant.CHECK_THREAD_MUTEX.acquire()
    if order_tx['tx_id'] + ":" + address in my_constant.THREAD_ORDERS:
        my_constant.THREAD_ORDERS.remove(order_tx['tx_id'] + ":" + address)
    my_constant.CHECK_THREAD_MUTEX.release()


def unlock_all_outputs(locked, ident_address):
    if locked is not None:
        try:
            my_constant.XVG_OUTPUTS_MUTEX.acquire()
            if ident_address in my_constant.XVG_LOCKED_BALANCE:
                my_constant.XVG_LOCKED_BALANCE.pop(ident_address)
            my_constant.XVG_OUTPUTS_MUTEX.release()
        except Exception as e:
            if my_constant.XVG_OUTPUTS_MUTEX.locked():
                my_constant.XVG_OUTPUTS_MUTEX.release()
            print_log("xvg:unlock_all_outputs: " + str(e), "WARNING", 3)


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
            print_log('xvg:transfer_to_address our balance was locked', 'WARNING', 3)
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
        print_log("xvg:transfer_to_address:" + str(e), "ERROR", 3)
        unlock_all_outputs(locked, order_tx['tx_id'] + ":" + address)
        return result


def partial_transfer(address, dst_address, dst_amt, locked, order_tx, miner_fee):
    tx_hash = transfer_verge(address, dst_address, dst_amt, locked, miner_fee)
    if tx_hash is None:
        return 1
    order_tx['tx'].append(tx_hash)
    order_tx['sent'] += dst_amt
    mongo.update_tx(address, order_tx)
    return 0


def transfer_verge(address, dst_address, dst_amount, locked, miner_fee):
    if locked is None:
        print_log("xvg:transfer_verge: locked is none address:" + address + " dest_address:" + dst_address, "WARNING", 3)
        return None
    inputs = []
    first_amount = floating(dst_amount - miner_fee, get_verge_decimals())
    outputs = [{dst_address: first_amount}]
    amount = 0
    for item in locked:
        vout = get_vout_from_unspend_tx(item['address'], item['tx_id'])
        if vout < 0:
            print_log("xvg:transfer_verge: already spent address:" + address + " dest_address:" + dst_address, "WARNING", 3)
            return None
        inputs.append({'txid': item['tx_id'], 'vout': vout})
        amount += item['amount']
    if amount < dst_amount:
        print_log("xvg:transfer_verge: exceed amount address:" + address + " dest_address:" + dst_address, "WARNING", 3)
        return None
    balance = floating(amount - first_amount - miner_fee, get_verge_decimals())
    if balance > 0:
        new_address = get_xvg_new_address()
        outputs.append({new_address: balance})
    tx_hash = send_xvg(inputs, outputs, miner_fee)
    if tx_hash is None:
        print_log("xvg:transfer_verge: transfer failed address:" + address + " dest_address:" + dst_address, "WARNING", 3)
        return None
    return tx_hash


def send_xvg(inputs, outputs, fee):
    my_constant.XVG_MUTEX.acquire()
    try:
        conn = get_connection()
        '''
        res = conn.settxfee(fee)
        if res is False:
            print_log("send_xvg: set mining fee: " + str(fee) + " FAILED", "ERROR", 5)
            my_constant.XVG_MUTEX.release()
            return None
        '''
        raw_tx = conn.createrawtransaction(inputs, outputs)
        signed_hex = conn.signrawtransaction(raw_tx)
        if signed_hex['complete'] is False:
            print_log("send_xvg: signing transaction failed  error:" + signed_hex['errors'], "NORMAL", 5)
            my_constant.XVG_MUTEX.release()
            return None
        tx_cw = conn.sendrawtransaction(signed_hex['hex'])
        my_constant.XVG_MUTEX.release()
        return tx_cw
    except Exception as e:
        print_log("send_xvg: " + str(e) + " inputs:" + str(inputs) + " outputs:" + str(outputs), "WARNING", 3)
        my_constant.XVG_MUTEX.release()
        return None


def get_vout_from_unspend_tx(address, tx_id):
    max_tries = 10
    tries = 0
    while tries < max_tries:
        try:
            tx_list = get_connection().listunspent(1, my_constant.VERGE_MAXIMUM_UNUSED_CONFIRM)
            for item in tx_list:
                if item.txid != tx_id:
                    continue
                if item.address != address:
                    continue
                if item.spendable is False:
                    return -1
                return item.vout
            break
        except Exception as e:
            print_log("xvg:get_vout_from_unspend_tx: " + str(e), "ERROR", 5)
            time.sleep(5)
    return -1


def get_verge_decimals():
    return my_constant.VERGE_DECIMALS['amount']


def get_amount_from_tx(address, tx):
    try:
        for item in tx.details:
            if item['category'] != 'receive':
                continue
            if item['address'] != address:
                continue
            return floating(item['amount'], get_verge_decimals())
        return 0
    except Exception as e:
        print_log("get_amount_from_tx:" + str(e), "ERROR", 3)
        return 0


def get_fee(tx):
    while True:
        try:
            priority = 'high ' + str(0.1)
            return priority
            '''
            conn = get_connection()
            medium_fee = conn.estimatesmartfee(6, BTC_MINER_FEE_ESTIMATE_MODE)
            raw_tx = conn.getrawtransaction(tx.txid, True)
            satoshi = math.fabs(float(tx['fee']) * 100000000) // raw_tx['size'] + 1
            if float(satoshi) >= float(medium_fee) * 100000000 / 1000:
                priority = 'high ' + str(tx['fee'])
            else:
                priority = 'low ' + str(tx['fee'])
            break
            '''
        except Exception as err:
            print_log("xvg:get_fee:" + str(err), "ERROR", 3)
            time.sleep(1)


def calc_min_confirms(amount):
    return 1


def make_transaction(address, tx, btc_miner_fee=0, ex_price=0):
    try:
        amount = get_amount_from_tx(address, tx)
        if amount <= 0:
            return None
        fee = get_fee(tx)
        confirm = int(tx.confirmations)
        min_conf = calc_min_confirms(amount)
        if confirm >= min_conf:
            status = "confirmed"
        else:
            status = "created"
        data = {'address': address, 'amount': amount, 'refunded': 0.0, 'confirming_time': '',
                'confirmations': confirm, 'tx_id': tx.txid, 'min_conf': min_conf, 'sent': 0,
                'time': datetime.now(), 'miner_fee': fee, 'processing_time': '',
                'step': 0, 'status': status, 'received': 0.0, 'comment': '', 'dst_amt': 0.0, 'took_time': 0,
                'tx_cw': {'amount': 0, 'address': '', 'tx_id': ''}, 'tx': [], 'tx_ref': [],
                'dest_miner_fee': my_bitcoin.get_dest_miner_fee(amount, ex_price, btc_miner_fee)}
        return data
    except Exception as e:
        print_log("make_transaction:" + str(e), "ERROR", 3)
        return None


def insert_and_append_list_xvg_tx(order, tx):
    try:
        tx_list = order['out_tx_list']
        btc_miner_fee = order['btc_miner_fee']
        ex_price = order['ex_price']
        for i in range(0, len(tx_list)):
            if tx_list[i]['tx_id'] == tx.txid:
                if tx_list[i]['confirmations'] < int(tx.confirmations):
                    tx_list[i]['confirmations'] = int(tx.confirmations)
                return tx_list[i]
        cust_tx = make_transaction(order['address'], tx, btc_miner_fee, ex_price)
        if cust_tx is None:
            return None
        tx_list.append(cust_tx)
        return cust_tx
    except Exception as e:
        print_log("insert_and_append_list_xvg_tx:" + str(e), "ERROR", 3)
        return None


def confirm_payment(order, tx):
    while True:
        try:
            if mongo.is_existed_out_tx_order(order['address'], tx) is False:
                return None
            conn = get_connection()
            item = conn.gettransaction(tx['tx_id'])
            amount = 0
            for detail in item.details:
                if detail['category'] != 'receive':
                    continue
                if detail['address'] == order['address']:
                    amount = detail['amount']
                    break
            if amount == 0:
                time.sleep(1)
                continue
            amount = floating(amount, get_verge_decimals())
            if int(item.confirmations) < calc_min_confirms(amount):
                time.sleep(1)
                continue
            tx['confirmations'] = int(item.confirmations)
            received = amount
            if received == 0:
                time.sleep(1)
                continue
            return floating(received, get_verge_decimals())
        except Exception as e:
            print_log(str(e), "ERROR", 3)
            time.sleep(1)


def start_transaction_thread(order, xvg_tx):
    if xvg_tx['step'] >= 8:
        return
    if order['source_coin'].upper() != 'XVG':
        return
    if order['dest_coin'].upper() == 'BTC':
        thread = threading.Thread(target=my_bitcoin.check_conf, args=(order, xvg_tx))
    else:
        return
    thread.start()


def check_completed_orders():
    while True:
        try:
            start_date = datetime.now() - timedelta(days=OLD_ORDER_CHECK_DAYS)
            orders = mongo.pull_by_date(start_date, ['completed', 'canceled'], 'XVG')
            if orders is None:
                time.sleep(0.1)
                continue
            for order in orders:
                lists = get_tx_list_by_address(order['address'])
                for item in lists:
                    try:
                        my_constant.TRANSACTION_MUTEX.acquire()
                        order = mongo.find_order_by_address(order['address'], 'XVG')
                        if order is None:
                            my_constant.TRANSACTION_MUTEX.release()
                            continue
                        xvg_tx = insert_and_append_list_xvg_tx(order, item)
                        if xvg_tx is None:
                            continue
                        mongo.update_out_tx_list(order['address'], order['out_tx_list'])
                        start_transaction_thread(order, xvg_tx)
                        my_constant.TRANSACTION_MUTEX.release()
                        time.sleep(0.01)
                    except Exception as e:
                        if my_constant.TRANSACTION_MUTEX.locked():
                            my_constant.TRANSACTION_MUTEX.release()
                        print_log("xvg:check_completed_orders locked1:" + str(e), "ERROR", 3)
                        continue
                # orders.append(order)
            time.sleep(10)
        except Exception as e:
            print_log("xvg:check_completed_orders:" + str(e), "ERROR", 3)
            time.sleep(10)


def check_transactions_thread():
    while True:
        try:
            orders = mongo.pull_by_status('created', 'XVG')
            if orders is None:
                time.sleep(0.1)
                continue
            for order in orders:
                lists = get_tx_list_by_address(order['address'])
                for item in lists:
                    try:
                        my_constant.TRANSACTION_MUTEX.acquire()
                        finding_order = mongo.find_order_by_address(order['address'], 'XVG')
                        if finding_order is None:
                            my_constant.TRANSACTION_MUTEX.release()
                            continue
                        order = finding_order
                        xvg_tx = insert_and_append_list_xvg_tx(order, item)
                        if xvg_tx is None:
                            continue
                        mongo.update_out_tx_list(order['address'], order['out_tx_list'])
                        start_transaction_thread(order, xvg_tx)
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
            print_log("xvg:check_transactions_thread:" + str(e), "ERROR", 3)
            time.sleep(0.1)


def get_tx_list_by_address(address):
    result = []
    try:
        conn = get_connection()
        lists = conn.listreceivedbyaddress(0)
        if lists is None:
            return result
        for tx_list in lists:
            if tx_list.address != address:
                continue
            conn = get_connection()
            for item in tx_list.txids:
                tx = conn.gettransaction(item)
                if tx is None:
                    continue
                result.append(tx)
        return result
    except Exception as e:
        print_log("xvg:get_tx_list_by_address:" + str(e), "ERROR", 3)
        return result


def update_order_amounts(order, btc_lists):
    try:
        received = 0.0
        refunded = 0.0
        dst_amt = 0.0
        completed = True
        received_usd = 0.0
        for item in order['out_tx_list']:
            if item['step'] == 0 and find_item_in_xvg_list(item, btc_lists) is False:
                mongo.update_bad_tx(item['address'], item)
                continue
            if item['amount'] < my_bitcoin.get_btc_reverse_min(order['source_coin'], order['ex_price']):
                continue
            received += item['amount']
            refunded += item['refunded']
            dst_amt += (item['dst_amt'] - item['dest_miner_fee'])
            if item['status'] == 'completed':
                received_usd += item['dst_amt'] * order['btc_usd']
            if item['step'] != 8:
                completed = False
        mongo.update_order_amount(order['address'], order['src_amt'], floating(dst_amt, my_bitcoin.get_btc_decimals()),
                                  floating(received, get_verge_decimals()), floating(refunded, get_verge_decimals()), floating(received_usd, 2))
        if received == 0:
            return 1  # cancelling
        if completed is True:
            return 0
        return -1
    except Exception as e:
        print_log("xvg:update_order_amounts:" + str(e), "ERROR", 3)
        return -2


def find_item_in_xvg_list(item, xvg_lists):
    found = False
    try:
        for xvg in xvg_lists:
            if xvg.txid != item['tx_id']:
                continue
            amount = get_amount_from_tx(item['address'], xvg)
            if amount <= 0:
                continue
            found = True
    except Exception as e:
        print_log("find_item_in_xvg_list:" + str(e), "ERROR", 3)
    return found


def refund_payment(address, dst_address, dst_amount, order_tx, miner_fee=0):
    locked = None
    try:
        while locked is None:
            locked = lock_outputs(dst_amount, 'ref_' + order_tx['tx_id'] + ":" + address)
            if is_enough_balance(dst_amount) is False:
                break
            time.sleep(10)
            print_log('xvg:refund_payment our balance was locked', 'WARNING', 3)
        if locked is None:
            print_log("xvg:refund_payment: locked is none address:" + address + " dest_address:" + dst_address,
                      "WARNING", 3)
            return None
        if miner_fee == 0:
            miner_fee = calc_miner_fee()
        inputs = []
        first_amount = floating(dst_amount - miner_fee, get_verge_decimals())
        outputs = [{dst_address: first_amount}]
        amount = 0
        for item in locked:
            vout = get_vout_from_unspend_tx(item['address'], item['tx_id'])
            if vout < 0:
                print_log("xvg:refund_payment: already spent address:" + address + " dest_address:" + dst_address,
                          "WARNING", 3)
                unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
                return None
            inputs.append({'txid': item['tx_id'], 'vout': vout})
            amount += item['amount']
        if amount < dst_amount:
            print_log("xvg:refund_payment: exceed amount address:" + address + " dest_address:" + dst_address,
                      "WARNING", 3)
            unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
            return None
        balance = floating(amount - first_amount - miner_fee, get_verge_decimals())
        if balance > 0:
            new_address = get_xvg_new_address()
            outputs.append({new_address: balance})
        tx_hash = send_xvg(inputs, outputs, miner_fee)
        if tx_hash is None:
            print_log("xvg:refund_payment: transfer failed address:" + address + " dest_address:" + dst_address,
                      "WARNING", 3)
            unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
            return None
        result = [tx_hash]
        unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
        return result
    except Exception as e:
        print_log('xvg:refund_payment:' + str(e), "ERROR", 3)
        unlock_all_outputs(locked, 'ref_' + order_tx['tx_id'] + ":" + address)
        return None


def load_balancing():
    while True:
        try:
            time.sleep(my_constant.XVG_LOAD_BALANCING_PERIOD)
            my_constant.XVG_OUTPUTS_MUTEX.acquire()
            conn = get_connection()
            lists = conn.listunspent(my_constant.XVG_MIN_LOAD_BALANCING_CONFIRM, my_constant.VERGE_MAXIMUM_UNUSED_CONFIRM)
            if len(lists) <= 0:
                my_constant.XVG_OUTPUTS_MUTEX.release()
                continue
            max_tx = lists[0]
            for item in lists:
                lock_value = {'address': str(item.address), 'tx_id': str(item.txid), 'amount': float(item.amount)}
                if is_reserve_tx(lock_value):
                    continue
                if max_tx.amount < item.amount:
                    max_tx = item
            if float(max_tx.amount) <= XVG_MIN_LOAD_BALANCING_AMOUNT:
                my_constant.XVG_OUTPUTS_MUTEX.release()
                print_log("xvg:load_balancing: not enough tx 0", "ERROR", 3)
                continue
            lock_value = {'address': str(max_tx.address), 'tx_id': str(max_tx.txid), 'amount': float(max_tx.amount)}
            ident_address = 'LOAD_BALANCING'
            if is_key_dict(my_constant.XVG_LOCKED_BALANCE, ident_address) is False:
                my_constant.XVG_LOCKED_BALANCE[ident_address] = []
            if is_reserve_tx_from_key(ident_address, lock_value) is True:
                my_constant.XVG_OUTPUTS_MUTEX.release()
                print_log("xvg:load_balancing: reserved tx", "ERROR", 3)
                continue
            counts = float(max_tx.amount) // XVG_MIN_LOAD_BALANCING_AMOUNT + 1
            if counts < 2:
                my_constant.XVG_OUTPUTS_MUTEX.release()
                print_log("xvg:load_balancing: not enough tx 1", "ERROR", 3)
                continue
            # xvg_miner_fee = floating(calc_miner_fee() * (counts - 1), 8)
            xvg_miner_fee = calc_miner_fee()
            if float(max_tx.amount) - xvg_miner_fee <= XVG_MIN_LOAD_BALANCING_AMOUNT:
                my_constant.XVG_OUTPUTS_MUTEX.release()
                print_log("xvg:load_balancing: not enough tx 2", "ERROR", 3)
                continue
            vout = get_vout_from_unspend_tx(str(max_tx.address), str(max_tx.txid))
            if vout < 0:
                my_constant.XVG_OUTPUTS_MUTEX.release()
                print_log("xvg:load_balancing: already spent tx", "ERROR", 3)
                continue
            my_constant.XVG_LOCKED_BALANCE[ident_address].append(lock_value)
            my_constant.XVG_OUTPUTS_MUTEX.release()
            try:
                inputs = [{'txid': str(max_tx.txid), 'vout': vout}]
                total_amount = float(max_tx.amount) - xvg_miner_fee
                amount = 0
                outputs = []
                while True:
                    amount += XVG_MIN_LOAD_BALANCING_AMOUNT
                    if amount >= total_amount:
                        rest_amount = floating(total_amount - (amount - XVG_MIN_LOAD_BALANCING_AMOUNT), get_verge_decimals())
                        new_address = get_xvg_new_address()
                        outputs.append({new_address: rest_amount})
                        break
                    new_address = get_xvg_new_address()
                    outputs.append({new_address: XVG_MIN_LOAD_BALANCING_AMOUNT})
                tx_cw = send_xvg(inputs, outputs, xvg_miner_fee)
                print_log("xvg:load_balancing: finished tx_cw:" + str(tx_cw), "ALARM", 3)
                unlock_all_outputs([lock_value], ident_address)
            except Exception as e:
                print_log("xvg:load_balancing: inner:" + str(e), "ERROR", 3)
        except Exception as e:
            print_log("xvg:load_balancing:" + str(e), "ERROR", 3)
            if my_constant.XVG_OUTPUTS_MUTEX.locked():
                my_constant.XVG_OUTPUTS_MUTEX.release()
