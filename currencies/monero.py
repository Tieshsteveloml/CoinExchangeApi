import threading
import time
from datetime import datetime, timedelta

import mongodb as mongo
from config.config import *
from currencies import bitcoin as my_bitcoin
from currencies.exchange import get_btc_xmr_exchange
from general import *


def create_wallet_addresses():
    my_constant.XMR_WALLET.refresh()
    current = len(my_constant.XMR_WALLET.accounts)
    if current >= LOCAL_ACCOUNT_COUNT:
        return LOCAL_ACCOUNT_COUNT
    for i in range(current, LOCAL_ACCOUNT_COUNT):
        try:
            new_account = my_constant.XMR_WALLET.new_account("account_" + str(i))
            if len(new_account.addresses()) <= 0:
                new_account.new_address()
        except Exception as e:
            print_log("monero:create_wallet_addresses:" + " " + str(e) + " account_" + str(i))
    return LOCAL_ACCOUNT_COUNT


def find_locked_tx(locked, tx):
    try:
        for item in my_constant.XMR_LOCKED_BALANCE[locked]['tx_id']:
            if item == tx:
                return True
        return False
    except Exception as e:
        print_log("find_locked_tx:" + str(e), "ERROR", 3)
        return False


def remove_locked_tx(locked, tx):
    try:
        find = find_locked_tx(locked, tx)
        if find:
            my_constant.XMR_LOCKED_BALANCE[locked]['tx_id'].remove(tx)
            return True
        return False
    except Exception as e:
        print_log("remove_locked_tx:" + str(e), "ERROR", 3)
        return False


def wallet_total_balance():
    balance = 0.0
    try:
        my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.XMR_WALLET.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = my_constant.XMR_WALLET.accounts[index]
            try:
                temp = float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[index]['locked'] - LIMIT_XMR_BALANCE
                balance += temp
            except Exception as e:
                print_log(str(e), "WARNING", 2)
    except Exception as err:
        print_log("monero:wallet_total_balance:" + str(err), "ERROR", 3)
    return balance


def wallet_max_balance():
    balance = 0.0
    try:
        my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.XMR_WALLET.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = my_constant.XMR_WALLET.accounts[index]
            try:
                temp = float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[index]['locked'] - LIMIT_XMR_BALANCE
                if balance < temp:
                    balance = temp
            except Exception as e:
                print_log(str(e), "WARNING", 2)
    except Exception as err:
        print_log("monero:max_balance:" + str(err), "ERROR", 3)
    return balance


def get_min_wallet_new_address():
    balance = 1000000000000
    min_index = 0
    mx_tries = 10
    try:
        my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.XMR_WALLET.accounts), LOCAL_ACCOUNT_COUNT)
        if count <= 0:
            return None
        my_constant.XMR_WORKING_WALLET_MUTEX.acquire()
        for index in range(0, count):
            account = my_constant.XMR_WALLET.accounts[index]
            try:
                if index in my_constant.XMR_WORKING_WALLET_INDEXES:
                    continue
                temp = float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[index]['locked'] - LIMIT_XMR_BALANCE
                if balance > temp:
                    balance = temp
                    min_index = index
            except Exception as e:
                print_log(str(e), "WARNING", 2)
        account = my_constant.XMR_WALLET.accounts[min_index]
        tried = 0
        while True:
            new_address = account.new_address()
            order = mongo.find_order_by_address(str(new_address[0]), 'XMR')
            if order is None:
                break
            time.sleep(1)
            tried += 1
            if tried >= mx_tries:
                my_constant.XMR_WORKING_WALLET_MUTEX.release()
                return None
        my_constant.XMR_WORKING_WALLET_INDEXES.append(min_index)
        my_constant.XMR_WORKING_WALLET_MUTEX.release()
        result = {'address': str(new_address[0]), 'indexes': str(min_index) + ":" + str(new_address[1])}
        return result
    except Exception as err:
        if my_constant.XMR_WORKING_WALLET_MUTEX.locked():
            my_constant.XMR_WORKING_WALLET_MUTEX.release()
        print_log("monero:max_balance:" + str(err), "ERROR", 3)
    return None


def find_proper_account(dst_amt):
    try:
        if MULTI_OUTPUTS:
            result = find_multi_account(dst_amt)
        else:
            result = find_best_account(dst_amt)
        return result
    except Exception as e:
        print_log("monero:find_proper_account:" + str(e), "ERROR", 3)
        return None


def max_to_min_layout(indexes):
    try:
        for i in range(0, len(indexes)):
            for j in range(i + 1, len(indexes)):
                account = my_constant.XMR_WALLET.accounts[indexes[i]]
                first = floating(
                    float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[indexes[i]]['locked'] - LIMIT_XMR_BALANCE, 3)
                account = my_constant.XMR_WALLET.accounts[indexes[j]]
                second = floating(
                    float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[indexes[j]]['locked'] - LIMIT_XMR_BALANCE, 3)
                if first >= second:
                    continue
                temp = indexes[i]
                indexes[i] = indexes[j]
                indexes[j] = temp
        print_log("monero:max_to_min_layout:" + str(indexes), "DEBUG", 1)
        return indexes
    except Exception as e:
        print_log("monero:max_to_min_layout:" + str(e), "ERROR", 3)
        return indexes


def find_multi_account(dst_amt):
    try:
        total = wallet_total_balance()
        if total < dst_amt:
            return None
        result = []
        possible = []
        # my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.XMR_WALLET.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = my_constant.XMR_WALLET.accounts[index]
            balance = float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[index]['locked'] - LIMIT_XMR_BALANCE
            if balance > dst_amt:
                possible.append(index)
        unlocked = []
        if len(possible) > 0:
            for i in possible:
                if len(my_constant.XMR_LOCKED_BALANCE[i]['tx_id']) == 0:
                    my_constant.XMR_LOCKED_BALANCE[i]['locked'] = 0.0
                    unlocked.append(i)
            if len(unlocked) > 0:
                max_balance = 0
                index = unlocked[0]
                for i in unlocked:
                    account = my_constant.XMR_WALLET.accounts[i]
                    balance = float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[i]['locked'] - LIMIT_XMR_BALANCE
                    if max_balance < balance:
                        max_balance = balance
                        index = i
            else:
                locked = 100000000
                index = possible[0]
                for i in possible:
                    if locked > len(my_constant.XMR_LOCKED_BALANCE[i]['tx_id']):
                        locked = len(my_constant.XMR_LOCKED_BALANCE[i]['tx_id'])
                        index = i
            data = {'index': index, 'amount': dst_amt}
            result.append(data)
            return result
        else:
            accum = 0.0
            locked = []
            for i in range(0, count):
                if len(my_constant.XMR_LOCKED_BALANCE[i]['tx_id']) == 0:
                    my_constant.XMR_LOCKED_BALANCE[i]['locked'] = 0.0
                    unlocked.append(i)
                else:
                    locked.append(i)
            unlocked = max_to_min_layout(unlocked)
            for i in unlocked:
                account = my_constant.XMR_WALLET.accounts[i]
                balance = floating(float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[i]['locked']
                                   - LIMIT_XMR_BALANCE, 3)
                if balance <= 0.0:
                    continue
                accum += balance
                if accum >= dst_amt:
                    data = {'index': i, 'amount': floating(balance - (accum - dst_amt), 3)}
                    result.append(data)
                    break
                data = {'index': i, 'amount': floating(balance, 3)}
                result.append(data)
            if accum >= dst_amt:
                return result
            locked = max_to_min_layout(locked)
            for i in locked:
                account = my_constant.XMR_WALLET.accounts[i]
                balance = floating(float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[i]['locked']
                                   - LIMIT_XMR_BALANCE, 3)
                if balance <= 0.0:
                    continue
                accum += balance
                if accum >= dst_amt:
                    data = {'index': i, 'amount': floating(balance - (accum - dst_amt), 3)}
                    result.append(data)
                    break
                data = {'index': i, 'amount': floating(balance, 3)}
                result.append(data)
            if accum >= dst_amt:
                return result
            return None
    except Exception as e:
        print_log("monero:find_multi_account:" + str(e), "ERROR", 3)
    return None


def find_best_account(dst_amt):
    try:
        result = []
        possible = []
        my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.XMR_WALLET.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = my_constant.XMR_WALLET.accounts[index]
            balance = float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[index]['locked'] - LIMIT_XMR_BALANCE
            if balance > dst_amt:
                possible.append(index)
        if len(possible) <= 0:
            return None
        unlocked = []
        for i in possible:
            if len(my_constant.XMR_LOCKED_BALANCE[i]['tx_id']) == 0:
                my_constant.XMR_LOCKED_BALANCE[i]['locked'] = 0.0
                unlocked.append(i)
        if len(unlocked) > 0:
            max_balance = 0
            index = unlocked[0]
            for i in unlocked:
                account = my_constant.XMR_WALLET.accounts[i]
                balance = float(account.balance()) - my_constant.XMR_LOCKED_BALANCE[i]['locked'] - LIMIT_XMR_BALANCE
                if max_balance < balance:
                    max_balance = balance
                    index = i
        else:
            locked = 100000000
            index = possible[0]
            for i in possible:
                if locked > len(my_constant.XMR_LOCKED_BALANCE[i]['tx_id']):
                    locked = len(my_constant.XMR_LOCKED_BALANCE[i]['tx_id'])
                    index = i
        data = {'index': index, 'amount': dst_amt}
        result.append(data)
        return result
    except Exception as e:
        print_log("monero:find_best_account:" + str(e), "ERROR", 3)
    return None


def lock_outputs(dst_amt, btc_tx, address):
    try:
        my_constant.XMR_OUTPUTS_MUTEX.acquire()
        locked = find_proper_account(dst_amt)
        if locked is not None:
            for item in locked:
                if find_locked_tx(item['index'], btc_tx['tx_id'] + ":" + address) is True:
                    continue
                my_constant.XMR_LOCKED_BALANCE[item['index']]['tx_id'].append(btc_tx['tx_id'] + ":" + address)
                my_constant.XMR_LOCKED_BALANCE[item['index']]['locked'] += item['amount']
        my_constant.XMR_OUTPUTS_MUTEX.release()
        return locked
    except Exception as e:
        if my_constant.XMR_OUTPUTS_MUTEX.locked():
            my_constant.XMR_OUTPUTS_MUTEX.release()
        print_log("monero:lock_outputs:" + str(e), "ERROR", 3)
        return None


def unlock_all_outputs(locked, btc_tx, address):
    if locked is not None:
        try:
            my_constant.XMR_OUTPUTS_MUTEX.acquire()
            for item in locked:
                if remove_locked_tx(item['index'], btc_tx['tx_id'] + ":" + address):
                    my_constant.XMR_LOCKED_BALANCE[item['index']]['locked'] -= item['amount']
            my_constant.XMR_OUTPUTS_MUTEX.release()
        except Exception as e:
            if my_constant.XMR_OUTPUTS_MUTEX.locked():
                my_constant.XMR_OUTPUTS_MUTEX.release()
            print_log("monero:unlock_all_outputs: " + str(e), "WARNING", 3)


def unlock_outputs(item, btc_tx, address):
    try:
        my_constant.XMR_OUTPUTS_MUTEX.acquire()
        if remove_locked_tx(item['index'], btc_tx['tx_id'] + ":" + address):
            my_constant.XMR_LOCKED_BALANCE[item['index']]['locked'] -= item['amount']
        my_constant.XMR_OUTPUTS_MUTEX.release()
    except Exception as e:
        if my_constant.XMR_OUTPUTS_MUTEX.locked():
            my_constant.XMR_OUTPUTS_MUTEX.release()
        print_log("monero:unlock_outputs:" + str(e), "ERROR", 3)


def partial_transfer(address, dst_address, locked, btc_tx):
    if locked is None:
        return 0
    step = 0
    for item in locked:
        while True:
            try:
                # time.sleep(3)
                my_constant.XMR_WALLET.refresh()
                print_log("monero:partial_transfer: from outputs(index:" + str(item['index']) + ") amount:"
                          + str(item['amount']) + " address:" + address, "NORMAL", 5)
                tx = my_constant.XMR_WALLET.accounts[item['index']].transfer(dst_address,
                                                                        item['amount'],
                                                                        priority=my_constant.XMR_TRANSFER_PRIORITY,
                                                                        unlock_time=0, relay=False)
                miner_fee = float(tx[0].fee)
                amount = item['amount'] - miner_fee
                tx = my_constant.XMR_WALLET.accounts[item['index']].transfer(dst_address,
                                                                            amount, priority=my_constant.XMR_TRANSFER_PRIORITY, unlock_time=0)

                btc_tx['tx'].append(str(tx[0]))
                btc_tx['sent'] += item['amount']
                btc_tx['sent'] = floating(btc_tx['sent'], 3)
                btc_tx['dest_miner_fee'] += miner_fee
                mongo.update_tx(address, btc_tx)
                unlock_outputs(item, btc_tx, address)
                break
            except Exception as err:
                print_log("monero:partial_transfer:" + str(err) + "from outputs(index:" +
                          str(item['index']) + ") amount:" + str(item['amount']) + " address:" + address, "ERROR", 3)
                str_err = str(err)
                if str_err == "not enough money":
                    unlock_outputs(item, btc_tx, address)
                    new_locked = lock_outputs(item['amount'], btc_tx, address)
                    step = partial_transfer(address, dst_address, new_locked, btc_tx)
                    break
                elif str_err.find('Address must be') >= 0:
                    unlock_outputs(item, btc_tx, address)
                    step = 3
                    break
                else:
                    details = "Queue, because our balance is locked."
                    mongo.update_transaction_status(address, btc_tx, "queue", details)
                time.sleep(3)
        if step == 3:
            break
    return step


def transfer_to_address(address, dst_address, dst_amt, btc_tx, refund_amt, received, ex_price, locked=None, step=0):
    result = {'step': step, 'refund_amt': refund_amt}
    if step != 0:
        return result
    try:
        if locked is None:
            locked = lock_outputs(dst_amt, btc_tx, address)
        if locked is None:
            refund_amt = received
            step = 1
            mongo.update_tx_step(address, btc_tx, step, refund_amt)
            result = {'step': step, 'refund_amt': refund_amt}
        else:
            step = partial_transfer(address, dst_address, locked, btc_tx)
            if step == 0:
                step = 2
            refund_amt = floating((btc_tx['dst_amt'] - btc_tx['sent']) / ex_price, 8)
            if refund_amt > received:
                refund_amt = received
            elif refund_amt < 0:
                refund_amt = 0
            mongo.update_tx_step(address, btc_tx, step, refund_amt)
            result = {'step': step, 'refund_amt': refund_amt}
        return result
    except Exception as e:
        print_log("monero:transfer_to_address:" + str(e), "ERROR", 3)
        return result


def check_conf(order, btc_tx):
    step = btc_tx['step']
    if step >= 8 or order['src_amt'] == 0:
        print_log(order['address'] + ":" + btc_tx['tx_id'] + " It is completed order. Thread is returned.")
        return
    #  print_log('thread started: ' + btc_tx['tx_id'], "DEBUG", 5)
    my_constant.CHECK_THREAD_MUTEX.acquire()
    if check_thread_order(btc_tx['tx_id'] + ":" + order['address']):
        my_constant.CHECK_THREAD_MUTEX.release()
        print_log(order['address'] + ":" + btc_tx['tx_id']
                  + " This transaction is already working on progress. Thread is returned.")
        return
    my_constant.THREAD_ORDERS.append(btc_tx['tx_id'] + ":" + order['address'])
    my_constant.CHECK_THREAD_MUTEX.release()
    dst_amt = my_bitcoin.get_dest_amount(btc_tx['amount'], order['ex_price'], order['btc_miner_fee'],
                                         EXCHANGE_FEE_DICT[order['dest_coin']] / 2, my_constant.MONERO_DECIMALS['amount'])
    btc_tx['dst_amt'] = dst_amt
    mongo.update_tx(order['address'], btc_tx)
    dst_amt -= btc_tx['sent']
    refund_amt = btc_tx['refunded']
    priority = btc_tx['miner_fee']
    status = get_order_status(order)
    if status == 'created' and step == 0 and dst_amt > 0:
        if priority[:3] != 'low' or DEBUG_MODE_TO_RESERVE:
            locked = lock_outputs(dst_amt, btc_tx, order['address'])
            if locked is None:
                ret = mongo.update_transaction_status(order['address'], btc_tx, "created", "exceed")
            else:
                ret = mongo.update_transaction_status(order['address'], btc_tx, "created", "reserve")
            if ret < 0:
                exit_thread(order['address'], locked, btc_tx)
                return
        else:
            locked = None
            ret = mongo.update_transaction_status(order['address'], btc_tx, "created", "low")
            if ret < 0:
                exit_thread(order['address'], locked, btc_tx)
                return
    else:
        locked = None
    received = btc_tx['received']
    while True:
        try:
            if received <= 0.0:
                received = my_bitcoin.confirm_payment(order, btc_tx)
                btc_tx['received'] = received
            if received <= 0.0:
                print_log(order['address'] + ":" + btc_tx['tx_id'] + " Order confirmation error. Thread is returned.")
                exit_thread(order['address'], locked, btc_tx)
                return
            confirmed = mongo.update_transaction_status(order['address'], btc_tx, "confirmed", '', True)
            if confirmed < 0:
                exit_thread(order['address'], locked, btc_tx)
                return
            print_log(
                order['address'] + ":" + btc_tx['tx_id'] + " Tx Confirmed. Amount:" + str(received)
                + " Confirmed Duration:" + str(confirmed) + "s", "NORMAL", 5)
            if received < my_bitcoin.get_btc_min(order['dest_coin']):
                unlock_all_outputs(locked, btc_tx, order['address'])
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
                mongo.update_tx_step(order['address'], btc_tx, step, refund_amt)
            elif step == 2:
                amount = floating(received - refund_amt, 8)
                tx_cw = my_bitcoin.bitcoin_split_process(order['address'], btc_tx['tx_id'], amount, EXCHANGE_FEE_DICT[order['dest_coin']] / 2, order['btc_miner_fee'])
                if tx_cw is not None:
                    btc_tx['tx_cw'] = tx_cw
                step = 4
                mongo.update_tx_step(order['address'], btc_tx, step, refund_amt)
                print_log(order['address'] + ":" + btc_tx['tx_id'] + " Tx Completed. Amount:" + str(order['src_amt'])
                          + " Received Amount:" + str(received), "NORMAL", 5)
                for item in btc_tx['tx']:
                    print_log(order['address'] + ":" + btc_tx['tx_id'] + " ==tx:" + str(item), "NORMAL", 5)
                print_log(order['address'] + ":" + btc_tx['tx_id'] + " tx_cw:" + str(btc_tx['tx_cw']), "NORMAL", 5)
                ret = mongo.update_transaction_status(order['address'], btc_tx, "completed", "Tx completed.")
                if ret < 0:
                    exit_thread(order['address'], locked, btc_tx)
                    return
            elif step == 0:
                if status == 'created':
                    if dst_amt > 0:
                        while my_constant.IS_XMR_LOAD_BALANCING is True:
                            details = "Queue, because our balance are being replenished, sorry for the inconvenience."
                            ret = mongo.update_transaction_status(order['address'], btc_tx, "queue", details)
                            if ret < 0:
                                exit_thread(order['address'], locked, btc_tx)
                                return
                            time.sleep(60)
                        result = transfer_to_address(order['address'], order['dst_address'], dst_amt, btc_tx, refund_amt, received, order['ex_price'],
                                                     locked, step)
                        step = result['step']
                        refund_amt = result['refund_amt']
                    else:
                        step = 2
                    if step == 2:
                        amount = floating(received - refund_amt, 8)
                        tx_cw = my_bitcoin.bitcoin_split_process(order['address'], btc_tx['tx_id'], amount,
                                                                 EXCHANGE_FEE_DICT[order['dest_coin']] / 2, order['btc_miner_fee'])
                        if tx_cw is not None:
                            btc_tx['tx_cw'] = tx_cw
                        step = 4
                        mongo.update_tx_step(order['address'], btc_tx, step, refund_amt)
                        print_log(
                            order['address'] + ":" + btc_tx['tx_id'] + " Tx Completed. Amount:" + str(order['src_amt'])
                            + " Received Amount:" + str(received), "NORMAL", 5)
                        for item in btc_tx['tx']:
                            print_log(order['address'] + ":" + btc_tx['tx_id'] + " ==tx:" + str(item), "NORMAL", 5)
                        print_log(order['address'] + ":" + btc_tx['tx_id'] + " tx_cw:" + str(btc_tx['tx_cw']), "NORMAL",
                                  5)
                        ret = mongo.update_transaction_status(order['address'], btc_tx, "completed", "Tx completed.")
                        if ret < 0:
                            exit_thread(order['address'], locked, btc_tx)
                            return
                else:
                    refund_amt = received
                    step = 5
                    mongo.update_tx_step(order['address'], btc_tx, step, refund_amt)
            refund_amt = float(refund_amt)
            if refund_amt > 0 and step <= 5:
                detail = ""
                while True:
                    try:
                        if payment == 3:
                            detail = "Received underpayment with below LIMIT_MIN_BTC."
                            step = 6
                            refund_amt = 0
                            mongo.update_tx_step(order['address'], btc_tx, step, refund_amt)
                            ret = mongo.update_transaction_status(order['address'], btc_tx, "deposit", detail)
                            if ret < 0:
                                exit_thread(order['address'], locked, btc_tx)
                                return
                            break
                        else:
                            tx_ref = my_bitcoin.refund_payment(order['address'], order['refund_address'], refund_amt, btc_tx, order['btc_miner_fee'])
                            if tx_ref is None:
                                print_log("check_conf transfer_refund_payment tx_ref is None " + "input_tx: " +
                                          btc_tx['tx_id'] + " dest: " + order['refund_address'] + " amount: " + str(refund_amt),
                                          "ERROR", 5)
                                time.sleep(5)
                                continue
                            btc_tx['tx_ref'] = tx_ref
                            try:
                                if step == 5:
                                    order = mongo.find_order_by_address(order['address'])
                                    refund = float(order['refunded_amt']) + refund_amt
                                    mongo.update_refunded_amount(order['address'], refund)
                                    detail = "Order was already timed out or completed."
                                elif step == 3:
                                    detail = "Invalid XMR address."
                                elif step == 4:
                                    detail = "Partial refunded, because not enough balance in our end."
                                else:
                                    detail = "Not enough balance in our end."
                                step = 7
                                mongo.update_tx_step(order['address'], btc_tx, step, refund_amt)
                                unlock_all_outputs(locked, btc_tx, order['address'])
                                ret = mongo.update_transaction_status(order['address'], btc_tx, "refunded", detail)
                                if ret < 0:
                                    exit_thread(order['address'], locked, btc_tx)
                                    return
                            except Exception as e:
                                print_log("4 DB updating error: " + str(e), "ERROR", 5)
                                detail = "DB updating error, but refunded completely"
                            break
                    except Exception as err:
                        print_log("4 " + str(err) + " Refund Address:" +
                                  order['refund_address'] + " Refund Amount:" + str(refund_amt), "ERROR", 5)
                        time.sleep(5)
                print_log(order['address'] + ":" + btc_tx['tx_id'] + " " + detail, "NORMAL", 5)
                for item in btc_tx['tx']:
                    print_log(order['address'] + ":" + btc_tx['tx_id'] + " ==tx:" + str(item), "NORMAL", 5)
                print_log(order['address'] + ":" + btc_tx['tx_id'] + " tx_cw:" + str(btc_tx['tx_cw']), "NORMAL", 5)
                print_log(order['address'] + ":" + btc_tx['tx_id'] + " tx_ref:" + str(btc_tx['tx_ref']), "NORMAL", 5)
            step = 8
            mongo.update_tx_step(order['address'], btc_tx, step, refund_amt)
            unlock_all_outputs(locked, btc_tx, order['address'])
            break
        except Exception as err:
            print_log("5 " + str(err), "ERROR", 5)
            time.sleep(10)
    my_constant.CHECK_THREAD_MUTEX.acquire()
    my_constant.THREAD_ORDERS.remove(btc_tx['tx_id'] + ":" + order['address'])
    my_constant.CHECK_THREAD_MUTEX.release()


def exit_thread(address, locked, btc_tx):
    if locked is not None:
        try:
            my_constant.XMR_OUTPUTS_MUTEX.acquire()
            for item in locked:
                if remove_locked_tx(item['index'], btc_tx['tx_id'] + ":" + address):
                    my_constant.XMR_LOCKED_BALANCE[item['index']]['locked'] -= item['amount']
            my_constant.XMR_OUTPUTS_MUTEX.release()
        except Exception as e:
            if my_constant.XMR_OUTPUTS_MUTEX.locked():
                my_constant.XMR_OUTPUTS_MUTEX.release()
            print_log("check_conf locked4: " + str(e), "WARNING", 3)
    my_constant.CHECK_THREAD_MUTEX.acquire()
    if btc_tx['tx_id'] + ":" + address in my_constant.THREAD_ORDERS:
        my_constant.THREAD_ORDERS.remove(btc_tx['tx_id'] + ":" + address)
    my_constant.CHECK_THREAD_MUTEX.release()


def split_list(array, split_count):
    try:
        size = len(array)
        if size <= split_count:
            return [array]
        idx_list = [idx for idx, val in enumerate(array) if idx % split_count == 0 and idx != 0]
        if len(idx_list) <= 0:
            return [array]
        res = [array[i: j] for i, j in zip([0] + idx_list, idx_list + ([size] if idx_list[-1] != size else []))]
        return res
    except Exception as e:
        print_log("split_list:" + str(e), "ERROR", 3)


def load_balancing_wallet():
    while True:
        try:
            my_constant.XMR_WALLET.refresh()
            current = len(my_constant.XMR_WALLET.accounts)
            index = 0
            if current > LOCAL_ACCOUNT_COUNT:
                for i in range(LOCAL_ACCOUNT_COUNT, current):
                    try:
                        if float(my_constant.XMR_WALLET.accounts[i].balance()) <= LIMIT_XMR_BALANCE:
                            continue
                        amount = floating(float(my_constant.XMR_WALLET.accounts[i].balance()) - LIMIT_XMR_BALANCE, 3)
                        if amount < LIMIT_XMR_BALANCE:
                            continue
                        if index >= LOCAL_ACCOUNT_COUNT:
                            index = 0
                        my_constant.XMR_WALLET.accounts[i].transfer(my_constant.XMR_WALLET.accounts[index].address(),
                                                                    amount, unlock_time=0)
                        index += 1
                        time.sleep(3)
                    except Exception as e:
                        print_log("monero:load_balancing_wallet:" + str(e), "WARNING", 3)
                        time.sleep(3)
            count = min(len(my_constant.XMR_WALLET.accounts), LOCAL_ACCOUNT_COUNT)
            if count <= 0:
                time.sleep(60)
                continue
            accum = 0.0
            for index in range(0, count):
                account = my_constant.XMR_WALLET.accounts[index]
                accum += floating(account.balance(), 3)
            average = accum / count
            if average >= 1:
                my_constant.IS_XMR_LOAD_BALANCING = True
                for index in range(0, count):
                    account = my_constant.XMR_WALLET.accounts[index]
                    if float(account.balance()) < count:
                        continue
                    rest = float(account.balance()) - average
                    if rest < average:
                        continue
                    locked = 0
                    try:
                        my_constant.XMR_OUTPUTS_MUTEX.acquire()
                        if my_constant.XMR_LOCKED_BALANCE[index]['locked'] > 0:
                            my_constant.XMR_OUTPUTS_MUTEX.release()
                            continue
                        locked = floating(account.balance(), 3)
                        my_constant.XMR_LOCKED_BALANCE[index]['tx_id'].append(account.address())
                        my_constant.XMR_LOCKED_BALANCE[index]['locked'] += locked
                        my_constant.XMR_OUTPUTS_MUTEX.release()
                    except Exception as e:
                        if my_constant.XMR_OUTPUTS_MUTEX.locked():
                            my_constant.XMR_OUTPUTS_MUTEX.release()
                        print_log("monero:load_balancing_wallet locked1: " + str(e), "WARNING", 3)
                    destination = []
                    for t in range(0, count):
                        if rest <= LIMIT_XMR_BALANCE:
                            break
                        if t == index:
                            continue
                        transfer = my_constant.XMR_WALLET.accounts[t]
                        lack = average - float(transfer.balance())
                        lack = min(lack, rest)
                        if lack <= LIMIT_XMR_BALANCE:
                            continue
                        data = (transfer.address(), floating(lack, 3))
                        destination.append(data)
                        rest -= lack
                    if len(destination) <= 0:
                        try:
                            my_constant.XMR_OUTPUTS_MUTEX.acquire()
                            if remove_locked_tx(index, account.address()):
                                my_constant.XMR_LOCKED_BALANCE[index]['locked'] -= locked
                            my_constant.XMR_OUTPUTS_MUTEX.release()
                        except Exception as e:
                            if my_constant.XMR_OUTPUTS_MUTEX.locked():
                                my_constant.XMR_OUTPUTS_MUTEX.release()
                            print_log("monero:load_balancing_wallet locked2: " + str(e), "WARNING", 3)
                        continue
                    desc_list = split_list(destination, 10)
                    for desc in desc_list:
                        while True:
                            try:
                                result = account.transfer_multiple(desc)
                                print_log("monero:load_balancing_wallet:" + str(result), "TRANSFER", 5)
                                time.sleep(30)
                                break
                            except Exception as e:
                                print_log("monero:load_balancing_wallet:" + str(e), "ERROR", 3)
                                str_err = str(e)
                                if str_err == "not enough money":
                                    break
                                time.sleep(30)
                    time.sleep(600)
                    try:
                        my_constant.XMR_OUTPUTS_MUTEX.acquire()
                        if remove_locked_tx(index, account.address()):
                            my_constant.XMR_LOCKED_BALANCE[index]['locked'] -= locked
                        my_constant.XMR_OUTPUTS_MUTEX.release()
                    except Exception as e:
                        if my_constant.XMR_OUTPUTS_MUTEX.locked():
                            my_constant.XMR_OUTPUTS_MUTEX.release()
                        print_log("monero:load_balancing_wallet locked3: " + str(e), "WARNING", 3)
                my_constant.IS_XMR_LOAD_BALANCING = False
            time.sleep(600)
        except Exception as e:
            print_log("monero:load_balancing_wallet:" + str(e), "ERROR", 3)
            time.sleep(600)


def get_btc_ex_price_from_url():
    try:
        prices = get_btc_xmr_exchange()
        if prices is None:
            return my_constant.EX_BTC_XMR_PRICE
        my_constant.EX_BTC_XMR_PRICE = prices
    except Exception as err:
        print_log("monero:get_btc_ex_price_from_url:" + str(err), "ERROR", 3)
    return my_constant.EX_BTC_XMR_PRICE


def get_ex_prices_thread():
    try:
        get_btc_ex_price_from_url()
        ex_price = get_btc_ex_price()
        max_btc = get_btc_max(ex_price)
        my_constant.XMR_MAX_BTC_BALANCE = max_btc
    except Exception as e:
        print_log("monero:get_ex_prices_thread:" + str(e), "ERROR", 3)


def get_btc_ex_price():
    while True:
        try:
            if my_constant.EX_BTC_XMR_PRICE['ex_price'] == 0.0:
                my_constant.EX_BTC_XMR_PRICE = get_btc_ex_price_from_url()
            return my_constant.EX_BTC_XMR_PRICE['ex_price']
        except Exception as e:
            print_log("monero:get_btc_ex_price:" + str(e), "ERROR", 3)


def get_btc_ex_reverse():
    while True:
        try:
            if my_constant.EX_BTC_XMR_PRICE['ex_reverse'] == 0.0:
                my_constant.EX_BTC_XMR_PRICE = get_btc_ex_price_from_url()
            return my_constant.EX_BTC_XMR_PRICE['ex_reverse']
        except Exception as e:
            print_log("monero:get_btc_ex_reverse:" + str(e), "ERROR", 3)


def get_btc_max(ex_price):
    if MULTI_OUTPUTS:
        max_bal = floating(wallet_total_balance() / ex_price, get_decimals(True))
    else:
        max_bal = floating(wallet_max_balance() / ex_price, get_decimals(True))
    return max_bal


def get_btc_max_balance():
    return my_constant.XMR_MAX_BTC_BALANCE


def get_decimals(reverse=False):
    if reverse:
        return my_constant.MONERO_DECIMALS['reverse']
    else:
        return my_constant.MONERO_DECIMALS['forward']


def start_transaction_thread(order, xmr_tx):
    if xmr_tx['step'] >= 8:
        return
    if order['source_coin'].upper() != 'XMR':
        return
    if order['dest_coin'].upper() == 'BTC':
        thread = threading.Thread(target=my_bitcoin.check_conf, args=(order, xmr_tx))
    else:
        return
    thread.start()


def confirm_payment(order, tx):
    while True:
        try:
            if mongo.is_existed_out_tx_order(order['address'], tx) is False:
                return None
            my_constant.XMR_WALLET.refresh()
            index = int(order['private'].split(':')[0])
            tx = find_tx_by_hash(order['address'], tx['tx_id'], index)
            if tx is None:
                return 0.0
            confirm = int(my_constant.XMR_WALLET.confirmations(tx))
            min_conf = calc_min_confirms(floating(tx.amount, my_constant.MONERO_DECIMALS['amount']))
            if confirm < min_conf:
                time.sleep(1)
                continue
            return floating(tx.amount, my_constant.MONERO_DECIMALS['amount'])
        except Exception as e:
            print_log("monero:confirm_payment:" + str(e), "ERROR", 3)
            time.sleep(1)


def find_tx_by_hash(address, tx_id, index):
    lists = my_constant.XMR_WALLET.accounts[index].incoming(unconfirmed=True, confirmed=True,
                                                            local_address=address)
    for item in lists:
        if item.transaction.hash == tx_id:
            return item
    return None


def check_completed_orders():
    while True:
        try:
            start_date = datetime.now() - timedelta(days=OLD_ORDER_CHECK_DAYS)
            orders = mongo.pull_by_date(start_date, ['completed', 'canceled'], 'XMR')
            for order in orders:
                index = int(order['private'].split(':')[0])
                my_constant.XMR_WALLET.refresh()
                lists = my_constant.XMR_WALLET.accounts[index].incoming(unconfirmed=True, confirmed=True,
                                                                        local_address=order['address'])
                for item in lists:
                    try:
                        my_constant.TRANSACTION_MUTEX.acquire()
                        finding_order = mongo.find_order_by_address(str(item.local_address), 'XMR')
                        if finding_order is None:
                            my_constant.TRANSACTION_MUTEX.release()
                            continue
                        order = finding_order
                        xmr_tx = insert_and_append_list_xmr_tx(order['out_tx_list'], item, order['btc_miner_fee'], order['ex_price'])
                        mongo.update_out_tx_list(str(item.local_address), order['out_tx_list'])
                        start_transaction_thread(order, xmr_tx)
                        my_constant.TRANSACTION_MUTEX.release()
                    except Exception as e:
                        if my_constant.TRANSACTION_MUTEX.locked():
                            my_constant.TRANSACTION_MUTEX.release()
                        print_log("xmr:check_completed_orders locked1:" + str(e), "ERROR", 3)
                        continue
                # orders.append(order)
            time.sleep(10)
        except Exception as e:
            print_log("xmr:check_completed_orders:" + str(e), "ERROR", 3)
            time.sleep(10)


def check_transactions_thread():
    while True:
        try:
            orders = mongo.pull_by_status('created', 'XMR')
            for order in orders:
                index = int(order['private'].split(':')[0])
                my_constant.XMR_WALLET.refresh()
                lists = my_constant.XMR_WALLET.accounts[index].incoming(unconfirmed=True, confirmed=True, local_address=order['address'])
                for item in lists:
                    try:
                        my_constant.TRANSACTION_MUTEX.acquire()
                        finding_order = mongo.find_order_by_address(str(item.local_address), 'XMR')
                        if finding_order is None:
                            my_constant.TRANSACTION_MUTEX.release()
                            continue
                        order = finding_order
                        xmr_tx = insert_and_append_list_xmr_tx(order['out_tx_list'], item, order['btc_miner_fee'], order['ex_price'])
                        mongo.update_out_tx_list(str(item.local_address), order['out_tx_list'])
                        start_transaction_thread(order, xmr_tx)
                        my_constant.TRANSACTION_MUTEX.release()
                    except Exception as e:
                        if my_constant.TRANSACTION_MUTEX.locked():
                            my_constant.TRANSACTION_MUTEX.release()
                        print_log("check_transactions_thread locked1:" + str(e), "ERROR", 3)
                        continue
                if len(order['out_tx_list']) == 0:
                    canceled = mongo.process_canceled_order(order, TIMEOUTS_ORDER['XMR'])
                    if canceled == 1:
                        my_constant.XMR_WORKING_WALLET_MUTEX.acquire()
                        if index in my_constant.XMR_WORKING_WALLET_INDEXES:
                            my_constant.XMR_WORKING_WALLET_INDEXES.remove(index)
                        my_constant.XMR_WORKING_WALLET_MUTEX.release()
                else:
                    res = update_order_amounts(order, lists)
                    if res == 0:
                        mongo.update_order_status(order['address'], 'completed', 'Completed order.')
                        my_constant.XMR_WORKING_WALLET_MUTEX.acquire()
                        if index in my_constant.XMR_WORKING_WALLET_INDEXES:
                            my_constant.XMR_WORKING_WALLET_INDEXES.remove(index)
                        my_constant.XMR_WORKING_WALLET_MUTEX.release()
                    elif res == 1:
                        canceled = mongo.process_canceled_order(order, TIMEOUTS_ORDER['XMR'])
                        if canceled == 1:
                            my_constant.XMR_WORKING_WALLET_MUTEX.acquire()
                            if index in my_constant.XMR_WORKING_WALLET_INDEXES:
                                my_constant.XMR_WORKING_WALLET_INDEXES.remove(index)
                            my_constant.XMR_WORKING_WALLET_MUTEX.release()
            time.sleep(0.1)
        except Exception as e:
            print_log("check_transactions_thread:" + str(e), "ERROR", 3)
            time.sleep(0.1)


def calc_min_confirms(amount):
    return 1


def get_fee_level(tx):
    try:
        trans = my_constant.XMR_WALLET.transfer(my_constant.XMR_TESTING_ADDRESS, tx.amount, priority=2, relay=False)
        safe_gas = float(trans[0].fee)
        if safe_gas > tx.transaction.fee:
            fee = 'low ' + str(tx.transaction.fee)
        else:
            fee = 'high ' + str(tx.transaction.fee)
        return fee
    except Exception as e:
        print_log('xmr:get_fee_level:' + str(e), 'ERROR', 3)
        return 'low ' + str(tx.transaction.fee)


def make_transaction(tx, btc_miner_fee=0, ex_price=0):
    try:
        fee = get_fee_level(tx)  # need to calculate fee level
        confirm = int(my_constant.XMR_WALLET.confirmations(tx))
        min_conf = calc_min_confirms(floating(tx.amount, my_constant.MONERO_DECIMALS['amount']))
        if confirm >= min_conf:
            status = "confirmed"
        else:
            status = "created"
        data = {'address': str(tx.local_address), 'amount': floating(tx.amount, my_constant.MONERO_DECIMALS['amount']), 'refunded': 0.0,
                'confirmations': confirm, 'tx_id': str(tx.transaction.hash), 'min_conf': min_conf, 'sent': 0,
                'time': datetime.now(), 'miner_fee': fee, 'processing_time': '', 'confirming_time': '',
                'step': 0, 'status': status, 'received': 0.0, 'comment': '', 'dst_amt': 0.0, 'took_time': 0,
                'tx_cw': {'amount': 0, 'address': '', 'tx_id': ''}, 'tx': [], 'tx_ref': [],
                'dest_miner_fee': my_bitcoin.get_dest_miner_fee(tx.amount, ex_price, btc_miner_fee)}
        return data
    except Exception as e:
        print_log("make_transaction:" + str(e), "ERROR", 3)
        return None


def insert_and_append_list_xmr_tx(tx_list, tx, btc_miner_fee=0, ex_price=0):
    try:
        for i in range(0, len(tx_list)):
            if tx_list[i]['tx_id'] == str(tx.transaction.hash):
                if tx_list[i]['confirmations'] < int(my_constant.XMR_WALLET.confirmations(tx)):
                    tx_list[i]['confirmations'] = int(my_constant.XMR_WALLET.confirmations(tx))
                return tx_list[i]
        order = make_transaction(tx, btc_miner_fee, ex_price)
        if order is None:
            return None
        tx_list.append(order)
        return order
    except Exception as e:
        print_log("insert_and_append_list_xmr_tx:" + str(e), "ERROR", 3)
        return None


def refund_payment(address, dst_address, dst_amt, order_tx, tx_ref):
    while True:
        try:
            locked = lock_outputs(dst_amt, order_tx, address)
            if locked is not None:
                break
            print_log("monero:refund_payment:lock_outputs is None", 'ERROR', 5)
            time.sleep(1)
        except Exception as e:
            print_log("monero:refund_payment:lock_outputs" + str(e), 'WARNING', 3)
    step = 0
    for item in locked:
        while True:
            try:
                my_constant.XMR_WALLET.refresh()
                print_log("monero:refund_payment: from outputs(index:" + str(item['index']) + ") amount:"
                          + str(item['amount']) + " address:" + address, "NORMAL", 5)
                tx = my_constant.XMR_WALLET.accounts[item['index']].transfer(dst_address,
                                                                             item['amount'],
                                                                             priority=my_constant.XMR_TRANSFER_PRIORITY,
                                                                             unlock_time=0, relay=False)
                amount = item['amount'] - float(tx[0].fee)
                tx = my_constant.XMR_WALLET.accounts[item['index']].transfer(dst_address,
                                                                            amount, priority=my_constant.XMR_TRANSFER_PRIORITY, unlock_time=0)
                tx_ref.append(str(tx[0]))
                unlock_outputs(item, order_tx, address)
                break
            except Exception as err:
                print_log("monero:refund_payment:" + str(err) + "from outputs(index:" +
                          str(item['index']) + ") amount:" + str(item['amount']) + " address:" + address, "ERROR", 3)
                str_err = str(err)
                if str_err == "not enough money":
                    unlock_outputs(item, order_tx, address)
                    refund_payment(address, dst_address, item['amount'], order_tx, tx_ref)
                    break
                elif str_err.find('Address must be') >= 0:
                    unlock_all_outputs(locked, order_tx, address)
                    step = 3
                    break
                else:
                    details = "Queue, because our balance is locked."
                    #  mongo.update_transaction_status(address, order_tx, "queue", details)
                time.sleep(3)
        if step == 3:
            break
    unlock_all_outputs(locked, order_tx, address)


def update_order_amounts(order, xmr_lists):
    try:
        received = 0.0
        refunded = 0.0
        dst_amt = 0.0
        completed = True
        received_usd = 0.0
        for item in order['out_tx_list']:
            if item['step'] == 0 and find_item_in_xmr_list(item, xmr_lists) is False:
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
                                  floating(received, 3), floating(refunded, 3), floating(received_usd, 2))
        if received == 0:
            return 1  # cancelling
        if completed is True:
            return 0
        return -1
    except Exception as e:
        print_log("xmr:update_order_amounts:" + str(e), "ERROR", 3)
        return -2


def find_item_in_xmr_list(item, xmr_lists):
    found = False
    try:
        for xmr in xmr_lists:
            if str(xmr.local_address) != item['address']:
                continue
            if str(xmr.transaction.hash) != item['tx_id']:
                continue
            found = True
    except Exception as e:
        print_log("find_item_in_xmr_list:" + str(e), "ERROR", 3)
    return found


def get_miner_fee(amount=1):
    try:
        if my_constant.XMR_MINER_FEE == 0:
            get_miner_fee_thread(amount)
        return  my_constant.XMR_MINER_FEE
    except Exception as e:
        print_log("get_miner_fee:" + str(e), "ERROR", 3)
        return 0


def get_miner_fee_thread(amount=1):
    try:
        my_constant.XMR_MINER_FEE = get_dest_miner_fee(amount)
    except Exception as e:
        print_log("get_miner_fee_thread:" + str(e), "ERROR", 3)
        return my_constant.XMR_MINER_FEE


def get_dest_miner_fee(amount=1):
    try:
        amount = floating(amount, my_constant.MONERO_DECIMALS['amount'])
        my_constant.XMR_WALLET.refresh()
        tx = my_constant.XMR_WALLET.transfer(my_constant.XMR_TESTING_ADDRESS, amount,
                                                                     priority=my_constant.XMR_TRANSFER_PRIORITY,
                                                                     unlock_time=0, relay=False)
        miner_fee = float(tx[0].fee)
        return miner_fee
    except Exception as e:
        print_log("xmr:get_dest_miner_fee:" + str(e), "ERROR", 3)
        return 0
