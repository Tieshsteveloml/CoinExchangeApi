from general import *
import mongodb as mongo
import time
from currencies import bitcoin as my_bitcoin
import random
import json
from .exchange import get_btc_yfi_exchange, get_btc_usdt_exchange, get_btc_vey_exchange, get_btc_wbtc_exchange, get_btc_eth_exchange
import requests
from config.config import *
from datetime import datetime, timedelta
from web3 import Web3
from eth_account import Account
import threading
from hexbytes import HexBytes


def get_eth_limit(gas_price=0, gas_limit=ETH_GAS_LIMIT):
    try:
        if gas_price == 0:
            gas_price = get_gas_price()
        gwei = gas_price * gas_limit
        return gwei / pow(10, 9)
    except Exception as e:
        print_log("ethereum:get_eth_limit:" + str(e))
        return 0.34


def create_wallet():
    while True:
        try:
            random_str = f'{random.randint(1000000000, 9999999999)}_{random.randint(1000000000, 9999999999)}'
            eth_account = Account.create(random_str)
            return eth_account
        except Exception as e:
            print_log("ethereum:create_wallet:" + str(e))
            time.sleep(1)


def create_wallet_addresses():
    current = len(my_constant.ETH_WEB3.eth.accounts)
    if current >= LOCAL_ACCOUNT_COUNT:
        return LOCAL_ACCOUNT_COUNT
    for i in range(current, LOCAL_ACCOUNT_COUNT):
        try:
            new_account = None
            while new_account is None:
                new_account = create_wallet()
            encrypted = Account.encrypt(new_account.privateKey, ETH_ACCOUNT_PASSWORD)
            f = open(ETH_KEYSTORE_PATH + str(i), 'w')
            f.write(json.dumps(encrypted))
            f.close()
        except Exception as e:
            print_log("ethereum:create_wallet_addresses:" + " " + str(e) + " account_" + str(i))
    return LOCAL_ACCOUNT_COUNT


def find_locked_tx(locked, tx):
    try:
        for item in my_constant.ETH_LOCKED_BALANCE[locked]['tx_id']:
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
            my_constant.ETH_LOCKED_BALANCE[locked]['tx_id'].remove(tx)
            return True
        return False
    except Exception as e:
        print_log("remove_locked_tx:" + str(e), "ERROR", 3)
        return False


def get_eth_balance(wallet_address):
    try:
        eth_balance = my_constant.ETH_WEB3.eth.getBalance(wallet_address)
        eth_balance = my_constant.ETH_WEB3.fromWei(eth_balance, 'ether')
        return float(eth_balance)
    except Exception as e:
        print_log("get_eth_balance:" + str(e), "ERROR", 3)
        return 0


def get_contract_abi(contract_addr):
    while True:
        try:
            try:
                abi = my_constant.ETH_CONTRACT_ABI[contract_addr]
                return abi
            except Exception as e:
                print_log("get_contract_abi:" + str(e), "DEBUG", 3)
                if USING_TEST_NET:
                    headers = {
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
                    response = requests.get('%s%s' % (my_constant.ABI_ENDPOINT, contract_addr), headers=headers)
                else:
                    response = requests.get('%s%s' % (my_constant.ABI_ENDPOINT, contract_addr))
                response_json = response.json()
                abi_json = json.loads(response_json['result'])
                result = json.dumps(abi_json)
                my_constant.ETH_CONTRACT_ABI[contract_addr] = result
                return result
        except Exception as e:
            print_log("get_contract_abi: contract: " + contract_addr + " " + str(e), "ERROR", 3)
            time.sleep(1)


def get_token_balance(wallet_address, token='YFI'):
    try:
        if token == 'ETH':
            return get_eth_balance(wallet_address)
        contract_address = my_constant.ETH_TOKEN_LIST[token]
        contract_abi = get_contract_abi(contract_address)
        contract_addr = Web3.toChecksumAddress(contract_address)
        contract = my_constant.ETH_WEB3.eth.contract(contract_addr, abi=contract_abi)
        decimals = get_decimals_token(token)
        token_balance = contract.functions.balanceOf(
            my_constant.ETH_WEB3.toChecksumAddress(wallet_address)).call() / pow(10, decimals)
        return float(token_balance)
    except Exception as e:
        print_log(token + ":get_token_balance:" + str(e), "ERROR", 3)
        return 0


def wallet_total_eth_balance():
    balance = 0.0
    try:
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = get_account_from_private_key(index)
            try:
                temp = float(get_eth_balance(account.address)) - my_constant.ETH_LOCKED_BALANCE[index]['eth']
                balance += temp
            except Exception as e:
                print_log(str(e), "WARNING", 2)
    except Exception as err:
        print_log("wallet_total_eth_balance:" + str(err), "ERROR", 3)
    return balance


def wallet_total_token_balance(token='YFI', eth_limit=0):
    balance = 0.0
    try:
        if token == 'ETH':
            return wallet_total_eth_balance()
        if eth_limit == 0:
            eth_limit = get_eth_limit()
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = get_account_from_private_key(index)
            try:
                eth = float(get_eth_balance(account.address)) - my_constant.ETH_LOCKED_BALANCE[index]['eth']
                if eth < eth_limit:
                    continue
                temp = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token]
                balance += temp
            except Exception as e:
                print_log(str(e), "WARNING", 2)
    except Exception as err:
        print_log(token + ":wallet_total_token_balance:" + str(err), "ERROR", 3)
    return balance


def wallet_max_eth_balance():
    balance = 0.0
    try:
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = get_account_from_private_key(index)
            try:
                temp = float(get_eth_balance(account.address)) - my_constant.ETH_LOCKED_BALANCE[index]['eth']
                if balance < temp:
                    balance = temp
            except Exception as e:
                print_log(str(e), "WARNING", 2)
    except Exception as err:
        print_log("max_eth_balance:" + str(err), "ERROR", 3)
    return balance


def find_min_token_wallet(token='YFI'):
    balance = 10000000
    result = 0
    try:
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = get_account_from_private_key(index)
            try:
                temp = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token]
                if balance > temp:
                    balance = temp
                    result = index
            except Exception as e:
                print_log(str(e), "WARNING", 2)
    except Exception as err:
        print_log(token + ":find_min_token_wallet:" + str(err), "ERROR", 3)
    return result


def wallet_max_token_balance(token="YFI", eth_limit=0):
    balance = 0.0
    try:
        if token == 'ETH':
            return wallet_max_eth_balance()
        if eth_limit == 0:
            eth_limit = get_eth_limit()
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = get_account_from_private_key(index)
            try:
                eth = float(get_eth_balance(account.address)) - my_constant.ETH_LOCKED_BALANCE[index]['eth']
                if eth < eth_limit:
                    continue
                temp = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token]
                if balance < temp:
                    balance = temp
            except Exception as e:
                print_log(str(e), "WARNING", 2)
    except Exception as err:
        print_log(token + ":max_token_balance:" + str(err), "ERROR", 3)
    return balance


def find_proper_account(dst_amt, token='YFI', gas_price=0):
    try:
        if MULTI_OUTPUTS:
            result = find_multi_account(dst_amt, token, gas_price)
        else:
            result = find_best_account(dst_amt, token, gas_price)
        return result
    except Exception as e:
        print_log(token + ":find_proper_account:" + str(e), "ERROR", 3)
        return None


def max_to_min_eth_layout(indexes):
    token = 'ETH'
    try:
        for i in range(0, len(indexes)):
            for j in range(i + 1, len(indexes)):
                account = get_account_from_private_key(indexes[i])
                first = floating(
                    float(get_token_balance(account.address, token)) -
                    my_constant.ETH_LOCKED_BALANCE[indexes[i]]['locked'][token] - my_constant.ETH_LOCKED_BALANCE[indexes[i]]['eth'], get_decimals_token(token))
                account = get_account_from_private_key(indexes[j])
                second = floating(
                    float(get_token_balance(account.address, token)) -
                    my_constant.ETH_LOCKED_BALANCE[indexes[j]]['locked'][token] - my_constant.ETH_LOCKED_BALANCE[indexes[j]]['eth'], get_decimals_token(token))
                if first >= second:
                    continue
                temp = indexes[i]
                indexes[i] = indexes[j]
                indexes[j] = temp
        print_log(token + ":max_to_min_eth_layout:" + str(indexes), "DEBUG", 1)
        return indexes
    except Exception as e:
        print_log(token + ":max_to_min_eth_layout:" + str(e), "ERROR", 3)
        return indexes


def max_to_min_layout(indexes, token='YFI'):
    try:
        for i in range(0, len(indexes)):
            for j in range(i + 1, len(indexes)):
                account = get_account_from_private_key(indexes[i])
                first = floating(
                    float(get_token_balance(account.address, token)) -
                    my_constant.ETH_LOCKED_BALANCE[indexes[i]]['locked'][token], get_decimals_token(token))
                account = get_account_from_private_key(indexes[j])
                second = floating(
                    float(get_token_balance(account.address, token)) -
                    my_constant.ETH_LOCKED_BALANCE[indexes[j]]['locked'][token], get_decimals_token(token))
                if first >= second:
                    continue
                temp = indexes[i]
                indexes[i] = indexes[j]
                indexes[j] = temp
        print_log(token + ":max_to_min_layout:" + str(indexes), "DEBUG", 1)
        return indexes
    except Exception as e:
        print_log(token + ":max_to_min_layout:" + str(e), "ERROR", 3)
        return indexes


def find_multi_eth_account(dst_amt, gas_price=0):
    token = "ETH"
    try:
        eth_limit = get_eth_limit(gas_price)  # need to define
        total = wallet_total_token_balance(token, eth_limit)
        if total < dst_amt:
            return None
        result = []
        possible = []
        # my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = get_account_from_private_key(index)
            balance = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token] - my_constant.ETH_LOCKED_BALANCE[index]['eth']
            balance -= eth_limit
            if balance > dst_amt:
                possible.append(index)
        unlocked = []
        if len(possible) > 0:
            for i in possible:
                if len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']) == 0:
                    my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] = 0.0
                    my_constant.ETH_LOCKED_BALANCE[i]['eth'] = 0.0
                    unlocked.append(i)
            if len(unlocked) > 0:
                max_balance = 0
                index = unlocked[0]
                for i in unlocked:
                    account = get_account_from_private_key(i)
                    balance = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] - my_constant.ETH_LOCKED_BALANCE[i]['eth']
                    if max_balance < balance:
                        max_balance = balance
                        index = i
            else:
                locked = 100000000
                index = possible[0]
                for i in possible:
                    if locked > len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']):
                        locked = len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id'])
                        index = i
            data = {'index': index, 'amount': dst_amt, 'eth_limit': eth_limit}
            result.append(data)
            return result
        else:
            accum = 0.0
            locked = []
            for i in range(0, count):
                if len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']) == 0:
                    my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] = 0.0
                    my_constant.ETH_LOCKED_BALANCE[i]['eth'] = 0.0
                    unlocked.append(i)
                else:
                    locked.append(i)
            unlocked = max_to_min_eth_layout(unlocked)
            for i in unlocked:
                account = get_account_from_private_key(i)
                balance = floating(float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] - my_constant.ETH_LOCKED_BALANCE[i]['eth'], get_decimals_token(token))
                balance -= eth_limit
                if balance <= 0.0:
                    continue
                accum += balance
                if accum >= dst_amt:
                    data = {'index': i, 'amount': floating(balance - (accum - dst_amt), get_decimals_token(token)), 'eth_limit': eth_limit}
                    result.append(data)
                    break
                data = {'index': i, 'amount': floating(balance, get_decimals_token(token)), 'eth_limit': eth_limit}
                result.append(data)
            if accum >= dst_amt:
                return result
            locked = max_to_min_eth_layout(locked)
            for i in locked:
                account = get_account_from_private_key(i)
                balance = floating(float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] - my_constant.ETH_LOCKED_BALANCE[i]['eth']
                                   , get_decimals_token(token))
                balance -= eth_limit
                if balance <= 0.0:
                    continue
                accum += balance
                if accum >= dst_amt:
                    data = {'index': i, 'amount': floating(balance - (accum - dst_amt), get_decimals_token(token)), 'eth_limit': eth_limit}
                    result.append(data)
                    break
                data = {'index': i, 'amount': floating(balance, get_decimals_token(token)), 'eth_limit': eth_limit}
                result.append(data)
            if accum >= dst_amt:
                return result
            return None
    except Exception as e:
        print_log(token + ":find_multi_eth_account:" + str(e), "ERROR", 3)
    return None


def find_multi_account(dst_amt, token="YFI", gas_price=0):
    try:
        if token == 'ETH':
            return find_multi_eth_account(dst_amt, gas_price)

        eth_limit = get_eth_limit(gas_price)  # need to define
        total = wallet_total_token_balance(token, eth_limit)
        if total < dst_amt:
            return None
        result = []
        possible = []
        # my_constant.XMR_WALLET.refresh()
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        for index in range(0, count):
            account = get_account_from_private_key(index)
            balance = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token]
            eth = float(get_eth_balance(account.address)) - my_constant.ETH_LOCKED_BALANCE[index]['eth']
            if balance > dst_amt and eth > eth_limit:
                possible.append(index)
        unlocked = []
        if len(possible) > 0:
            for i in possible:
                if len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']) == 0:
                    my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] = 0.0
                    my_constant.ETH_LOCKED_BALANCE[i]['eth'] = 0.0
                    unlocked.append(i)
            if len(unlocked) > 0:
                max_balance = 0
                index = unlocked[0]
                for i in unlocked:
                    account = get_account_from_private_key(i)
                    balance = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[i]['locked'][token]
                    if max_balance < balance:
                        max_balance = balance
                        index = i
            else:
                locked = 100000000
                index = possible[0]
                for i in possible:
                    if locked > len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']):
                        locked = len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id'])
                        index = i
            data = {'index': index, 'amount': dst_amt, 'eth_limit': eth_limit}
            result.append(data)
            return result
        else:
            accum = 0.0
            locked = []
            for i in range(0, count):
                if len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']) == 0:
                    my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] = 0.0
                    my_constant.ETH_LOCKED_BALANCE[i]['eth'] = 0.0
                    unlocked.append(i)
                else:
                    locked.append(i)
            unlocked = max_to_min_layout(unlocked, token)
            for i in unlocked:
                account = get_account_from_private_key(i)
                balance = floating(float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[i]['locked'][token], get_decimals_token(token))
                if balance <= 0.0:
                    continue
                eth = float(get_eth_balance(account.address)) - my_constant.ETH_LOCKED_BALANCE[i]['eth']
                if eth < eth_limit:
                    continue
                accum += balance
                if accum >= dst_amt:
                    data = {'index': i, 'amount': floating(balance - (accum - dst_amt), get_decimals_token(token)), 'eth_limit': eth_limit}
                    result.append(data)
                    break
                data = {'index': i, 'amount': floating(balance, get_decimals_token(token)), 'eth_limit': eth_limit}
                result.append(data)
            if accum >= dst_amt:
                return result
            locked = max_to_min_layout(locked, token)
            for i in locked:
                account = get_account_from_private_key(i)
                balance = floating(float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[i]['locked'][token]
                                   , get_decimals_token(token))
                if balance <= 0.0:
                    continue
                eth = float(get_eth_balance(account.address)) - my_constant.ETH_LOCKED_BALANCE[i]['eth']
                if eth < eth_limit:
                    continue
                accum += balance
                if accum >= dst_amt:
                    data = {'index': i, 'amount': floating(balance - (accum - dst_amt), get_decimals_token(token)), 'eth_limit': eth_limit}
                    result.append(data)
                    break
                data = {'index': i, 'amount': floating(balance, get_decimals_token(token)), 'eth_limit': eth_limit}
                result.append(data)
            if accum >= dst_amt:
                return result
            return None
    except Exception as e:
        print_log(token + ":find_multi_account:" + str(e), "ERROR", 3)
    return None


def find_best_eth_account(dst_amt, gas_price=0):
    token = 'ETH'
    try:
        result = []
        possible = []
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        eth_limit = get_eth_limit(gas_price)  # need to define
        for index in range(0, count):
            account = get_account_from_private_key(index)
            balance = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token] - my_constant.ETH_LOCKED_BALANCE[index]['eth']
            balance -= eth_limit
            if balance > dst_amt:
                possible.append(index)
        if len(possible) <= 0:
            return None
        unlocked = []
        for i in possible:
            if len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']) == 0:
                my_constant.ETH_LOCKED_BALANCE[i]['eth'] = 0.0
                my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] = 0.0
                unlocked.append(i)
        if len(unlocked) > 0:
            max_balance = 0
            index = unlocked[0]
            for i in unlocked:
                account = get_account_from_private_key(i)
                balance = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] - my_constant.ETH_LOCKED_BALANCE[i]['eth']
                if max_balance < balance:
                    max_balance = balance
                    index = i
        else:
            locked = 100000000
            index = possible[0]
            for i in possible:
                if locked > len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']):
                    locked = len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id'])
                    index = i
        data = {'index': index, 'amount': dst_amt, 'eth_limit': eth_limit}
        result.append(data)
        return result
    except Exception as e:
        print_log(token + ":find_best_account:" + str(e), "ERROR", 3)
    return None


def find_best_account(dst_amt, token='YFI', gas_price=0):
    try:
        if token == 'ETH':
            return find_best_eth_account(dst_amt, gas_price)
        result = []
        possible = []
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        eth_limit = get_eth_limit(gas_price)  # need to define
        for index in range(0, count):
            account = get_account_from_private_key(index)
            balance = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token]
            eth = float(get_eth_balance(account.address)) - my_constant.ETH_LOCKED_BALANCE[index]['eth']
            if balance > dst_amt and eth > eth_limit:
                possible.append(index)
        if len(possible) <= 0:
            return None
        unlocked = []
        for i in possible:
            if len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']) == 0:
                my_constant.ETH_LOCKED_BALANCE[i]['eth'] = 0.0
                my_constant.ETH_LOCKED_BALANCE[i]['locked'][token] = 0.0
                unlocked.append(i)
        if len(unlocked) > 0:
            max_balance = 0
            index = unlocked[0]
            for i in unlocked:
                account = get_account_from_private_key(i)
                balance = float(get_token_balance(account.address, token)) - my_constant.ETH_LOCKED_BALANCE[i]['locked'][token]
                if max_balance < balance:
                    max_balance = balance
                    index = i
        else:
            locked = 100000000
            index = possible[0]
            for i in possible:
                if locked > len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id']):
                    locked = len(my_constant.ETH_LOCKED_BALANCE[i]['tx_id'])
                    index = i
        data = {'index': index, 'amount': dst_amt, 'eth_limit': eth_limit}
        result.append(data)
        return result
    except Exception as e:
        print_log(token + ":find_best_account:" + str(e), "ERROR", 3)
    return None


def lock_outputs(dst_amt, btc_tx, address, token='YFI', gas_price=0):
    try:
        my_constant.ETH_OUTPUTS_MUTEX.acquire()
        if my_constant.IS_ETH_LOAD_BALANCING:
            my_constant.ETH_OUTPUTS_MUTEX.release()
            return None
        locked = find_proper_account(dst_amt, token, gas_price)
        if locked is not None:
            for item in locked:
                if find_locked_tx(item['index'], btc_tx['tx_id'] + ":" + address) is True:
                    continue
                my_constant.ETH_LOCKED_BALANCE[item['index']]['tx_id'].append(btc_tx['tx_id'] + ":" + address)
                my_constant.ETH_LOCKED_BALANCE[item['index']]['locked'][token] += item['amount']
                my_constant.ETH_LOCKED_BALANCE[item['index']]['eth'] += item['eth_limit']
        my_constant.ETH_OUTPUTS_MUTEX.release()
        return locked
    except Exception as e:
        if my_constant.ETH_OUTPUTS_MUTEX.locked():
            my_constant.ETH_OUTPUTS_MUTEX.release()
        print_log(token + ":lock_outputs:" + str(e), "ERROR", 3)
        return None


def unlock_all_outputs(locked, btc_tx, address, token='YFI'):
    if locked is not None:
        try:
            my_constant.ETH_OUTPUTS_MUTEX.acquire()
            for item in locked:
                if remove_locked_tx(item['index'], btc_tx['tx_id'] + ":" + address):
                    my_constant.ETH_LOCKED_BALANCE[item['index']]['locked'][token] -= item['amount']
                    my_constant.ETH_LOCKED_BALANCE[item['index']]['eth'] -= item['eth_limit']
            my_constant.ETH_OUTPUTS_MUTEX.release()
        except Exception as e:
            if my_constant.ETH_OUTPUTS_MUTEX.locked():
                my_constant.ETH_OUTPUTS_MUTEX.release()
            print_log(token + ":unlock_all_outputs: " + str(e), "WARNING", 3)


def unlock_outputs(item, btc_tx, address, token='YFI'):
    try:
        my_constant.ETH_OUTPUTS_MUTEX.acquire()
        if remove_locked_tx(item['index'], btc_tx['tx_id'] + ":" + address):
            my_constant.ETH_LOCKED_BALANCE[item['index']]['locked'][token] -= item['amount']
            my_constant.ETH_LOCKED_BALANCE[item['index']]['eth'] -= item['eth_limit']
        my_constant.ETH_OUTPUTS_MUTEX.release()
    except Exception as e:
        if my_constant.ETH_OUTPUTS_MUTEX.locked():
            my_constant.ETH_OUTPUTS_MUTEX.release()
        print_log(token + ":unlock_outputs:" + str(e), "ERROR", 3)


def get_decimals_token(token='YFI'):
    try:
        if my_constant.ETH_TOKENS_INFO[token]['decimals'] == 0:
            if token == 'ETH':
                my_constant.ETH_TOKENS_INFO[token]['decimals'] = 18
            else:
                contract_addr = Web3.toChecksumAddress(my_constant.ETH_TOKEN_LIST[token])
                contract_abi = get_contract_abi(my_constant.ETH_TOKEN_LIST[token])
                contract = my_constant.ETH_WEB3.eth.contract(contract_addr, abi=contract_abi)
                my_constant.ETH_TOKENS_INFO[token]['decimals'] = contract.functions.decimals().call()
        return my_constant.ETH_TOKENS_INFO[token]['decimals']
    except Exception as e:
        print_log(token + ":get_decimals_token:" + str(e), "ERROR", 3)
        return my_constant.ETH_TOKENS_INFO[token]['decimals']


def transfer_eth(source_address, source_private_key, dest_address, amount, gas_limit, gas_price, wait=False):
    result = {'code': -1, 'tx': None, 'message': ''}
    try:
        eth_balance = my_constant.ETH_WEB3.eth.getBalance(source_address)
        eth_limit = gas_limit * my_constant.ETH_WEB3.toWei(gas_price, 'gwei')
        value = my_constant.ETH_WEB3.toWei(amount, 'ether')
        #  eth_balance = get_eth_balance(source_address)
        #  eth_limit = gas_limit * gas_price / pow(10, 9)
        if eth_balance + pow(10, 9) < value + eth_limit:
            result['code'] = -3
            result['message'] = 'not enough money'
            result['tx'] = None
            return result
        elif eth_balance < value + eth_limit:
            value = eth_balance - eth_limit
        # ---------- sign and do transaction ---------- #
        signed_txn = my_constant.ETH_WEB3.eth.account.signTransaction(dict(
                        nonce=my_constant.ETH_WEB3.eth.getTransactionCount(source_address),
                        gasPrice=my_constant.ETH_WEB3.toWei(gas_price, 'gwei'),
                        gas=gas_limit,
                        to=dest_address,
                        value=value
                      ), private_key=source_private_key)
        txn_hash = my_constant.ETH_WEB3.eth.sendRawTransaction(signed_txn.rawTransaction)

        # @FIXME ----- check if transaction is success ----- #
        print_log('transfer_eth: hash:' + txn_hash.hex(), 'ALARM', 5)
        if wait is True:
            txn_receipt = my_constant.ETH_WEB3.eth.waitForTransactionReceipt(txn_hash, ETH_LIMIT_WAIT_TIME)
            if txn_receipt is None or 'status' not in txn_receipt or txn_receipt['status'] != 1 or 'transactionIndex' not in txn_receipt:
                result['code'] = -4
                result['message'] = 'waiting failed'
                result['tx'] = txn_hash.hex()
                return result
        result['code'] = 0
        result['message'] = ''
        result['tx'] = txn_hash.hex()
        return result
    except Exception as e:
        print_log("transfer_eth:" + str(e), "ERROR", 3)
        result['code'] = -2
        result['message'] = str(e)
        result['tx'] = None
        return result


def transfer_token(source_address, source_private_key, dest_address, amount, gas_limit, gas_price, token='YFI', wait=False):
    result = {'code': -1, 'tx': None, 'message': ''}
    try:
        # ---------- get contract object ---------- #
        contract_addr = Web3.toChecksumAddress(my_constant.ETH_TOKEN_LIST[token])
        contract_abi = get_contract_abi(my_constant.ETH_TOKEN_LIST[token])
        contract = my_constant.ETH_WEB3.eth.contract(contract_addr, abi=contract_abi)
        decimals = get_decimals_token(token)
        # ---------- check source wallet balance ---------- #
        source_balance = contract.functions.balanceOf(source_address).call()
        print_log(token + ':transfer_token : balance:' + str(source_balance) + " amount:" + str(amount) + ' decimals:' + str(decimals), 'DEBUG', 3)
        value = int(amount * pow(10, decimals))
        if source_balance + 10 < value:
            result['code'] = -3
            result['message'] = 'not enough money'
            result['tx'] = None
            return result
        elif source_balance < value:
            value = source_balance
        gwei = gas_price * gas_limit
        required_gas = gwei / pow(10, 9)
        if get_eth_balance(source_address) < floating(required_gas, 17):
            result['code'] = -4
            result['message'] = 'not enough gas'
            result['tx'] = None
            return result
        # ---------- make transaction hash object ---------- #

        tx_hash = contract.functions.transfer(dest_address, value).buildTransaction({
            'chainId': 1,
            'gasPrice': my_constant.ETH_WEB3.toWei(gas_price, 'gwei'),
            'gas': gas_limit,
            'nonce': my_constant.ETH_WEB3.eth.getTransactionCount(source_address),
        })

        # ---------- sign and do transaction ---------- #
        signed_txn = my_constant.ETH_WEB3.eth.account.signTransaction(tx_hash, private_key=source_private_key)
        txn_hash = my_constant.ETH_WEB3.eth.sendRawTransaction(signed_txn.rawTransaction)
        if wait is True:
            txn_receipt = my_constant.ETH_WEB3.eth.waitForTransactionReceipt(txn_hash, ETH_LIMIT_WAIT_TIME)
            if txn_receipt is None or 'status' not in txn_receipt or txn_receipt['status'] != 1 or 'transactionIndex' not in txn_receipt:
                result['code'] = -5
                result['message'] = 'waiting failed'
                result['tx'] = txn_hash.hex()
                return result
        result['code'] = 0
        result['message'] = ''
        result['tx'] = txn_hash.hex()
        return result
    except Exception as e:
        print_log(token + ":transfer_token:" + str(e), "ERROR", 3)
        result['code'] = -2
        result['message'] = str(e)
        result['tx'] = None
        return result


def get_account_from_private_key(index):
    try:
        if is_key_dict(my_constant.ETH_ACCOUNT_DICT, index) is False:
            with open(ETH_KEYSTORE_PATH + str(index)) as f:
                key_json = f.read()
            f.close()
            private_key = Account.decrypt(key_json, ETH_ACCOUNT_PASSWORD)
            acct = Account.privateKeyToAccount(private_key)
            my_constant.ETH_ACCOUNT_DICT[index] = acct
        else:
            acct = my_constant.ETH_ACCOUNT_DICT[index]
        return acct
    except Exception as e:
        print_log("get_account_private_key:" + str(e), "ERROR", 3)
        return None


def get_gas_price(level=''):
    while True:
        try:
            if level != '':
                res = requests.get(my_constant.ETH_GAS_URL).json()
                return int(res[level] / 10)
            if ETH_GAS_FROM_API:
                res = requests.get(my_constant.ETH_GAS_URL).json()
                if level == '':
                    return int(res[ETH_GAS_LEVEL] / 10)
                else:
                    return int(res[level] / 10)
            else:
                res = my_constant.ETH_WEB3.fromWei(my_constant.ETH_WEB3.eth.gasPrice, 'gwei')
                return int(res)
        except Exception as e:
            print_log("get_gas_price:" + str(e), "ERROR", 3)
            time.sleep(1)


def partial_transfer(address, dst_address, locked, btc_tx, token='YFI', gas_price=0):
    if locked is None:
        return 0
    step = 0
    for item in locked:
        while True:
            try:
                # time.sleep(3)
                print_log(token + ":partial_transfer: from outputs(index:" + str(item['index']) + ") amount:"
                          + str(item['amount']) + " address:" + address, "NORMAL", 5)
                source_account = get_account_from_private_key(item['index'])
                if source_account is None:
                    print_log(token + ":partial_transfer: failed, not get source address private key", "ERROR", 3)
                    time.sleep(3)
                    continue
                if gas_price == 0:
                    gas_price = item['eth_limit'] * pow(10, 9) / ETH_GAS_LIMIT

                if token == 'ETH':
                    tx_hash = transfer_eth(source_account.address, source_account.privateKey, dst_address,
                                             item['amount'], ETH_GAS_LIMIT, gas_price, True)
                else:
                    tx_hash = transfer_token(source_account.address, source_account.privateKey, dst_address,
                                             item['amount'], ETH_GAS_LIMIT, gas_price, token, True)
                if tx_hash['code'] == 0:
                    btc_tx['tx'].append(tx_hash['tx'])
                    btc_tx['sent'] += item['amount']
                    btc_tx['sent'] = floating(btc_tx['sent'], get_decimals_token(token))
                    mongo.update_tx(address, btc_tx)
                    unlock_outputs(item, btc_tx, address, token)
                    break
                elif tx_hash['code'] > -3:
                    print_log(token + ":partial_transfer:" + tx_hash['message'] + " from outputs(index:" +
                              str(item['index']) + ") amount:" + str(item['amount']) + " address:" + address, "ERROR",
                              3)
                    if tx_hash['message'].find('Could not find address') >= 0:
                        unlock_outputs(item, btc_tx, address, token)
                        step = 3
                        break
                    else:
                        details = "Queue, because our balance is locked."
                        mongo.update_transaction_status(address, btc_tx, "queue", details)
                        time.sleep(3)
                elif tx_hash['code'] > -10:
                    print_log(token + ":partial_transfer critical:" + tx_hash['message'] + " from outputs(index:" +
                              str(item['index']) + ") amount:" + str(item['amount']) + " address:" + address, "ERROR",
                              3)
                    unlock_outputs(item, btc_tx, address, token)
                    new_locked = lock_outputs(item['amount'], btc_tx, address, token, gas_price)
                    step = partial_transfer(address, dst_address, new_locked, btc_tx, token, gas_price)
                    break
            except Exception as err:
                print_log(token + ":partial_transfer:" + str(err), "ERROR", 3)
                time.sleep(3)
        if step == 3:
            break
    return step


def transfer_to_address(address, dst_address, dst_amt, btc_tx, refund_amt, received, ex_price, locked=None, step=0, token='YFI', gas_price=0):
    result = {'step': step, 'refund_amt': refund_amt}
    if step != 0:
        return result
    try:
        if locked is None:
            locked = lock_outputs(dst_amt, btc_tx, address, token, gas_price)
        if locked is None:
            refund_amt = received
            step = 1
            mongo.update_tx_step(address, btc_tx, step, refund_amt)
            result = {'step': step, 'refund_amt': refund_amt}
        else:
            step = partial_transfer(address, dst_address, locked, btc_tx, token, gas_price)
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
        print_log(token + ":transfer_to_address:" + str(e), "ERROR", 3)
        return result


def check_conf(order, btc_tx):
    step = btc_tx['step']
    if step >= 8 or order['src_amt'] == 0:
        print_log(order['address'] + ":" + btc_tx['tx_id'] + " It is completed order. Thread is returned.")
        return
    my_constant.CHECK_THREAD_MUTEX.acquire()
    if check_thread_order(btc_tx['tx_id'] + ":" + order['address']):
        my_constant.CHECK_THREAD_MUTEX.release()
        print_log(order['address'] + ":" + btc_tx['tx_id']
                  + " This transaction is already working on progress. Thread is returned.")
        return
    my_constant.THREAD_ORDERS.append(btc_tx['tx_id'] + ":" + order['address'])
    my_constant.CHECK_THREAD_MUTEX.release()
    dst_amt = my_bitcoin.get_dest_amount(btc_tx['amount'], order['ex_price'], order['btc_miner_fee'],
                                         EXCHANGE_FEE_DICT[order['dest_coin']] / 2,
                                         get_decimals_token(order['dest_coin']), order['dest_coin'], order['gas_price'])
    btc_tx['dst_amt'] = dst_amt
    mongo.update_tx(order['address'], btc_tx)
    dst_amt -= btc_tx['sent']
    refund_amt = btc_tx['refunded']
    priority = btc_tx['miner_fee']
    status = get_order_status(order)
    if status == 'created' and step == 0 and dst_amt > 0:
        if priority[:3] != 'low' or DEBUG_MODE_TO_RESERVE:
            locked = lock_outputs(dst_amt, btc_tx, order['address'], order['dest_coin'], order['gas_price'])
            if locked is None:
                ret = mongo.update_transaction_status(order['address'], btc_tx, "created", "exceed")
            else:
                ret = mongo.update_transaction_status(order['address'], btc_tx, "created", "reserve")
            if ret < 0:
                exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
                return
        else:
            locked = None
            ret = mongo.update_transaction_status(order['address'], btc_tx, "created", "low")
            if ret < 0:
                exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
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
                exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
                return
            confirmed = mongo.update_transaction_status(order['address'], btc_tx, "confirmed", '', True)
            if confirmed < 0:
                exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
                return
            print_log(
                order['address'] + ":" + btc_tx['tx_id'] + " Tx Confirmed. Amount:" + str(received)
                + " Confirmed Duration:" + str(confirmed) + "s", "NORMAL", 5)
            if received < my_bitcoin.get_btc_min(order['dest_coin']):
                unlock_all_outputs(locked, btc_tx, order['address'], order['dest_coin'])
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
                tx_cw = my_bitcoin.bitcoin_split_process(order['address'], btc_tx['tx_id'], amount,
                                                         EXCHANGE_FEE_DICT[order['dest_coin']] / 2, order['btc_miner_fee'])
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
                    exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
                    return
            elif step == 0:
                if status == 'created':
                    if dst_amt > 0:
                        while my_constant.IS_ETH_LOAD_BALANCING is True:
                            details = "Queue, because our balance are being replenished, sorry for the inconvenience."
                            ret = mongo.update_transaction_status(order['address'], btc_tx, "queue", details)
                            if ret < 0:
                                exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
                                return
                            time.sleep(60)
                        result = transfer_to_address(order['address'], order['dst_address'], dst_amt, btc_tx, refund_amt, received, order['ex_price'],
                                                     locked, step, order['dest_coin'], order['gas_price'])
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
                            exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
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
                                exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
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
                                    detail = "Invalid ETH address."
                                elif step == 4:
                                    detail = "Partial refunded, because not enough balance in our end."
                                else:
                                    detail = "Not enough balance in our end."
                                step = 7
                                mongo.update_tx_step(order['address'], btc_tx, step, refund_amt)
                                unlock_all_outputs(locked, btc_tx, order['address'], order['dest_coin'])
                                ret = mongo.update_transaction_status(order['address'], btc_tx, "refunded", detail)
                                if ret < 0:
                                    exit_thread(order['address'], locked, btc_tx, order['dest_coin'])
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
            unlock_all_outputs(locked, btc_tx, order['address'], order['dest_coin'])
            break
        except Exception as err:
            print_log("5 " + str(err), "ERROR", 5)
            time.sleep(10)
    my_constant.CHECK_THREAD_MUTEX.acquire()
    my_constant.THREAD_ORDERS.remove(btc_tx['tx_id'] + ":" + order['address'])
    my_constant.CHECK_THREAD_MUTEX.release()


def exit_thread(address, locked, btc_tx, token='YFI'):
    if locked is not None:
        try:
            my_constant.ETH_OUTPUTS_MUTEX.acquire()
            for item in locked:
                if remove_locked_tx(item['index'], btc_tx['tx_id'] + ":" + address):
                    my_constant.ETH_LOCKED_BALANCE[item['index']]['locked'][token] -= item['amount']
                    my_constant.ETH_LOCKED_BALANCE[item['index']]['eth'] -= item['eth_limit']
            my_constant.ETH_OUTPUTS_MUTEX.release()
        except Exception as e:
            if my_constant.ETH_OUTPUTS_MUTEX.locked():
                my_constant.ETH_OUTPUTS_MUTEX.release()
            print_log(token + ":exit_thread: " + str(e), "WARNING", 3)
    my_constant.CHECK_THREAD_MUTEX.acquire()
    if btc_tx['tx_id'] + ":" + address in my_constant.THREAD_ORDERS:
        my_constant.THREAD_ORDERS.remove(btc_tx['tx_id'] + ":" + address)
    my_constant.CHECK_THREAD_MUTEX.release()


def load_balancing_token_wallet(token='YFI'):
    try:
        current = len(my_constant.ETH_WEB3.eth.accounts)
        index = 0
        eth_limit = get_eth_limit()
        gas_price = eth_limit * pow(10, 9) / ETH_GAS_LIMIT
        if current > LOCAL_ACCOUNT_COUNT:
            for i in range(LOCAL_ACCOUNT_COUNT, current):
                try:
                    source_account = get_account_from_private_key(i)
                    eth_balance = get_eth_balance(source_account.address)
                    if eth_balance <= eth_limit:
                        continue
                    token_balance = get_token_balance(source_account.address, token)
                    if token_balance <= 0:
                        continue
                    if index >= LOCAL_ACCOUNT_COUNT:
                        index = 0
                    result = transfer_token(source_account.address, source_account.privatekey,
                                            my_constant.ETH_WEB3.eth.accouts[index], token_balance, ETH_GAS_LIMIT,
                                            gas_price, token, True)
                    if result['code'] != 0:
                        print_log(token + ':load_balancing_token_wallet:exceed1:' + result['message'], 'ERROR', 3)
                        continue
                    index += 1
                except Exception as e:
                    print_log(token + ":load_balancing_token_wallet:exceed2:" + str(e), "WARNING", 3)
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        if count <= 0:
            return
        accum = 0.0
        for index in range(0, count):
            account = get_account_from_private_key(index)
            token_balance = get_token_balance(account.address, token) - my_constant.ETH_LOCKED_BALANCE[index]['locked'][token]
            accum += token_balance
        average = accum / count
        if average >= 1:
            my_constant.ETH_TOKEN_LOAD_BALANCING_MUTEX.acquire()
            for index in range(0, count):
                my_constant.ETH_OUTPUTS_MUTEX.acquire()
                if len(my_constant.ETH_LOCKED_BALANCE[index]['tx_id']) > 0:
                    my_constant.ETH_OUTPUTS_MUTEX.release()
                    continue
                source = get_account_from_private_key(index)
                eth_limit = get_eth_limit()
                gas_price = eth_limit * pow(10, 9) / ETH_GAS_LIMIT
                eth_balance = get_eth_balance(source.address)
                if eth_balance < eth_limit:
                    my_constant.ETH_OUTPUTS_MUTEX.release()
                    continue
                token_balance = get_token_balance(source.address, token)
                if token_balance < float(count) * ETH_LOAD_BALANCING_PERCENT / 100.0 * average:
                    my_constant.ETH_OUTPUTS_MUTEX.release()
                    continue
                rest = token_balance - average
                if rest < average:
                    my_constant.ETH_OUTPUTS_MUTEX.release()
                    continue
                my_constant.IS_ETH_LOAD_BALANCING = True
                my_constant.ETH_OUTPUTS_MUTEX.release()
                for t in range(0, count):
                    if t == index:
                        continue
                    transfer = get_account_from_private_key(t)
                    eth_balance = get_eth_balance(source.address)
                    if eth_balance < eth_limit:
                        break
                    t_token = get_token_balance(transfer.address, token)
                    lack = average - t_token
                    lack = min(lack, rest)
                    if lack <= 0:
                        continue
                    result = transfer_token(source.address, source.privateKey, transfer.address, lack,
                                            ETH_GAS_LIMIT, gas_price, token, True)
                    print_log(token + ":load_balancing_token_wallet:code:" + str(result['code'])
                              + " message:" + str(result['message']), "TRANSFER", 5)
                    rest = rest - lack
                    if rest <= 0:
                        break
                my_constant.IS_ETH_LOAD_BALANCING = False
            my_constant.ETH_TOKEN_LOAD_BALANCING_MUTEX.release()
    except Exception as e:
        print_log(token + ":load_balancing_token_wallet:" + str(e), "ERROR", 3)
        my_constant.ETH_TOKEN_LOAD_BALANCING_MUTEX.release()


def load_balancing_wallet():
    while True:
        try:
            if len(mongo.pull_by_status('created')) > 0:
                time.sleep(60)
                continue
            for token in my_constant.ETH_TOKEN_LIST.keys():
                if token == 'ETH':
                    load_balancing_eth_wallet()
                load_balancing_token_wallet(token)
            time.sleep(600)
        except Exception as e:
            print_log("load_balancing_wallet:" + str(e), "ERROR", 3)
            time.sleep(600)


def load_balancing_eth_wallet():
    try:
        current = len(my_constant.ETH_WEB3.eth.accounts)
        index = 0
        eth_limit = get_eth_limit()
        gas_price = eth_limit * pow(10, 9) / ETH_GAS_LIMIT
        if current > LOCAL_ACCOUNT_COUNT:
            for i in range(LOCAL_ACCOUNT_COUNT, current):
                try:
                    source_account = get_account_from_private_key(i)
                    eth_balance = get_eth_balance(source_account.address)
                    if eth_balance <= eth_limit:
                        continue
                    amount = floating(eth_balance - eth_limit, 18)
                    if amount <= 0:
                        continue
                    if index >= LOCAL_ACCOUNT_COUNT:
                        index = 0
                    result = transfer_eth(source_account.address, source_account.privatekey,
                                          my_constant.ETH_WEB3.eth.accouts[index], amount, ETH_GAS_LIMIT, gas_price,
                                          True)
                    if result['code'] != 0:
                        print_log('load_balancing_eth_wallet:exceed1:' + result['message'], 'ERROR', 3)
                        continue
                    index += 1
                except Exception as e:
                    print_log("load_balancing_eth_wallet:exceed2:" + str(e), "WARNING", 3)
        count = min(len(my_constant.ETH_WEB3.eth.accounts), LOCAL_ACCOUNT_COUNT)
        if count <= 0:
            return
        accum = 0.0
        for index in range(0, count):
            account = get_account_from_private_key(index)
            eth_balance = get_eth_balance(account.address) - my_constant.ETH_LOCKED_BALANCE[index]['eth']
            accum += eth_balance
        average = accum / count
        if average >= ETH_LOAD_BALANCING_THRESHOLD:
            my_constant.ETH_TOKEN_LOAD_BALANCING_MUTEX.acquire()
            for index in range(0, count):
                my_constant.ETH_OUTPUTS_MUTEX.acquire()
                if len(my_constant.ETH_LOCKED_BALANCE[index]['tx_id']) > 0:
                    my_constant.ETH_OUTPUTS_MUTEX.release()
                    continue
                source = get_account_from_private_key(index)
                eth_balance = get_eth_balance(source.address)
                if eth_balance < average * float(count) * ETH_LOAD_BALANCING_PERCENT / 100.0:
                    my_constant.ETH_OUTPUTS_MUTEX.release()
                    continue
                eth_limit = get_eth_limit()
                gas_price = eth_limit * pow(10, 9) / ETH_GAS_LIMIT
                rest = eth_balance - average - eth_limit
                # if rest < average:
                #    continue
                my_constant.IS_ETH_LOAD_BALANCING = True
                my_constant.ETH_OUTPUTS_MUTEX.release()
                for t in range(0, count):
                    if t == index:
                        continue
                    transfer = get_account_from_private_key(t)
                    transfer_balance = get_eth_balance(transfer.address)
                    lack = average - transfer_balance
                    lack = min(lack, rest)
                    if lack <= 0:
                        continue
                    result = transfer_eth(source.address, source.privateKey, transfer.address, lack,
                                          ETH_GAS_LIMIT, gas_price, True)
                    print_log("load_balancing_eth_wallet:code:" + str(result['code'])
                              + " message:" + str(result['message']), "TRANSFER", 5)
                    eth_bal = get_eth_balance(source.address)
                    rest = eth_bal - average - eth_limit
                    if rest <= 0:
                        break
                my_constant.IS_ETH_LOAD_BALANCING = False
            my_constant.ETH_TOKEN_LOAD_BALANCING_MUTEX.release()
    except Exception as e:
        print_log("load_balancing_eth_wallet:" + str(e), "ERROR", 3)
        my_constant.ETH_TOKEN_LOAD_BALANCING_MUTEX.release()


def get_ex_price_thread():
    for token in my_constant.ETH_TOKEN_LIST.keys():
        get_btc_ex_price_from_url(token)
        calc_max_btc(token)
        time.sleep(1)


def get_btc_ex_price_from_url(token='YFI'):
    try:
        if token == 'YFI':
            price = get_btc_yfi_exchange()
        elif token == 'USDT':
            price = get_btc_usdt_exchange()
        elif token == 'VEY':
            price = get_btc_vey_exchange()
        elif token == 'WBTC':
            price = get_btc_wbtc_exchange()
        elif token == 'ETH':
            price = get_btc_eth_exchange()
        else:
            price = get_btc_yfi_exchange()
        if price['ex_price'] != 0:
            my_constant.ETH_TOKENS_INFO[token]['btc_ex_price']['ex_price'] = floating(price['ex_price'], get_btc_ex_decimals(token))
            my_constant.ETH_TOKENS_INFO[token]['btc_ex_price']['ex_reverse'] = floating(price['ex_reverse'], get_btc_ex_decimals(token, True))
        return my_constant.ETH_TOKENS_INFO[token]['btc_ex_price']
    except Exception as err:
        print_log(token + ":get_btc_ex_price_from_url:" + str(err), "ERROR", 3)
    return my_constant.ETH_TOKENS_INFO[token]['btc_ex_price']


def get_btc_ex_decimals(token, reverse=False):
    if reverse:
        return my_constant.ETH_TOKEN_DECIMALS[token]['reverse']
    else:
        return my_constant.ETH_TOKEN_DECIMALS[token]['forward']


def get_btc_ex_price(token='YFI'):
    while True:
        try:
            if my_constant.ETH_TOKENS_INFO[token]['btc_ex_price']['ex_price'] == 0.0:
                my_constant.ETH_TOKENS_INFO[token]['btc_ex_price'] = get_btc_ex_price_from_url(token)
            return my_constant.ETH_TOKENS_INFO[token]['btc_ex_price']['ex_price']
        except Exception as e:
            print_log(token + ":get_btc_ex_price:" + str(e), "ERROR", 3)
            time.sleep(1)


def get_btc_ex_reverse(token='YFI'):
    while True:
        try:
            if my_constant.ETH_TOKENS_INFO[token]['btc_ex_price']['ex_reverse'] == 0.0:
                my_constant.ETH_TOKENS_INFO[token]['btc_ex_price'] = get_btc_ex_price_from_url(token)
            return my_constant.ETH_TOKENS_INFO[token]['btc_ex_price']['ex_reverse']
        except Exception as e:
            print_log(token + ":get_btc_ex_reverse:" + str(e), "ERROR", 3)
            time.sleep(1)


def calc_max_btc(token='YFI'):
    ex_price = get_btc_ex_price(token)
    max_btc = get_btc_max(ex_price, token)
    my_constant.ETH_TOKENS_INFO[token]['max_btc'] = max_btc


def get_btc_max(ex_price, token='YFI'):
    eth_limit = get_eth_limit()  # need to define
    if MULTI_OUTPUTS:
        max_bal = floating(wallet_total_token_balance(token, eth_limit) / ex_price, 8)
    else:
        max_bal = floating(wallet_max_token_balance(token, eth_limit) / ex_price, 8)
    return max_bal


def get_btc_max_balance(token='YFI'):
    return my_constant.ETH_TOKENS_INFO[token]['max_btc']


def detect_transactions(block, tr, token, to_address, result):
    for x in range(0, tr):
        try:
            trans = my_constant.ETH_WEB3.eth.getTransactionByBlock(block, x)
            value = trans['value']
            input_info = trans['input']
            # Check if transaction is a contract transfer
            if value == 0 and not input_info.startswith('0xa9059cbb'):
                continue
            gas = my_constant.ETH_WEB3.eth.getTransactionReceipt(trans['hash'])['gasUsed']
            if token == '' or token == 'ETH':
                if value == 0:
                    continue
                if trans['to'] is None:
                    continue
                if to_address.lower() != trans['to'].lower():
                    continue
                value = my_constant.ETH_WEB3.fromWei(value, 'ether')
                data = {'tx': trans, 'token': '', 'amount': float(value), 'gas_price': trans['gasPrice'], 'gas': gas, 'address': to_address}
                result.append(data)
                continue
            elif input_info.startswith('0xa9059cbb'):
                if trans['to'] is None:
                    continue
                if my_constant.ETH_TOKEN_LIST[token].lower() != trans['to'].lower():
                    continue
                contract_to = input_info[10:-64].lower()
                target = to_address[2:].lower()
                if contract_to.find(target) < 0:
                    continue
                contract_value = int(input_info[74:], 16)
                contract_value = contract_value / pow(10, get_decimals_token(token))
                data = {'tx': trans, 'token': token, 'amount': contract_value, 'gas_price': trans['gasPrice'], 'gas': gas, 'address': to_address}
                result.append(data)
                continue
        except Exception as e:
            print_log("detect_transactions token:" + token + " address:" + to_address + " " + str(e), "ERROR", 3)


def detect_blocks(order, result):
    try:
        end_block = int(my_constant.ETH_WEB3.eth.blockNumber)
        begin_block = order['begin_block']
        if begin_block + my_constant.ETH_LIMIT_BEGIN_BLOCK < end_block:
            begin_block = end_block - my_constant.ETH_BEFORE_DETECT_BLOCK
        else:
            begin_block -= my_constant.ETH_LIMIT_MIN_BLOCK
        for block in range(begin_block, end_block):
            transactions = my_constant.ETH_WEB3.eth.getBlockTransactionCount(block)
            detect_transactions(block, transactions, order['source_coin'], order['address'], result)
        mongo.update_order_begin_block(order['address'], end_block)
    except Exception as e:
        print_log("detect_blocks token:" + order['source_coin'] + " address:" + order['address'] + " " + str(e), "ERROR", 3)


def start_transaction_thread(order, trans):
    if trans['step'] >= 8:
        return
    if is_key_dict(my_constant.ETH_TOKEN_LIST, order['source_coin']) is False:
        return
    if order['dest_coin'].upper() == 'BTC':
        thread = threading.Thread(target=my_bitcoin.check_conf, args=(order, trans))
    else:
        return
    thread.start()


def check_completed_orders():
    while True:
        try:
            start_date = datetime.now() - timedelta(days=OLD_ORDER_CHECK_DAYS)
            orders = mongo.pull_by_date(start_date, ['completed', 'canceled'])
            for order in orders:
                if is_key_dict(my_constant.ETH_TOKEN_LIST, order['source_coin']) is False:
                    continue
                lists = []
                detect_blocks(order, lists)
                for item in lists:
                    try:
                        my_constant.TRANSACTION_MUTEX.acquire()
                        finding_order = mongo.find_order_by_address(order['address'])
                        if finding_order is None:
                            my_constant.TRANSACTION_MUTEX.release()
                            continue
                        order = finding_order
                        trans = insert_and_append_list_tx(order['out_tx_list'], item, order['btc_miner_fee'], order['ex_price'])
                        mongo.update_out_tx_list(order['address'], order['out_tx_list'])
                        start_transaction_thread(order, trans)
                        my_constant.TRANSACTION_MUTEX.release()
                    except Exception as e:
                        if my_constant.TRANSACTION_MUTEX.locked():
                            my_constant.TRANSACTION_MUTEX.release()
                        print_log("eth:check_completed_orders locked1:" + str(e), "ERROR", 3)
                        continue
            time.sleep(10)
        except Exception as e:
            print_log("eth:check_completed_orders:" + str(e), "ERROR", 3)
            time.sleep(10)


def check_transactions_thread():
    while True:
        try:
            orders = mongo.pull_by_status('created')
            for order in orders:
                if is_key_dict(my_constant.ETH_TOKEN_LIST, order['source_coin']) is False:
                    continue
                lists = []
                detect_blocks(order, lists)
                for item in lists:
                    try:
                        my_constant.TRANSACTION_MUTEX.acquire()
                        finding_order = mongo.find_order_by_address(order['address'])
                        if finding_order is None:
                            my_constant.TRANSACTION_MUTEX.release()
                            continue
                        order = finding_order
                        trans = insert_and_append_list_tx(order['out_tx_list'], item, order['btc_miner_fee'], order['ex_price'])
                        mongo.update_out_tx_list(order['address'], order['out_tx_list'])
                        my_constant.TRANSACTION_MUTEX.release()
                        # start_transaction_thread(order, trans)
                    except Exception as e:
                        if my_constant.TRANSACTION_MUTEX.locked():
                            my_constant.TRANSACTION_MUTEX.release()
                        print_log("check_transactions_thread locked1:" + str(e), "ERROR", 3)
                        continue
                my_constant.TRANSACTION_MUTEX.acquire()
                finding_order = mongo.find_order_by_address(order['address'])
                if finding_order is None:
                    my_constant.TRANSACTION_MUTEX.release()
                    continue
                order = finding_order
                if len(order['out_tx_list']) == 0:
                    my_constant.TRANSACTION_MUTEX.release()
                    mongo.process_canceled_order(order, TIMEOUTS_ORDER[order['source_coin']])
                else:
                    for tx in order['out_tx_list']:
                        start_transaction_thread(order, tx)
                    my_constant.TRANSACTION_MUTEX.release()
                    res = update_order_amounts(order)
                    if res == 0:
                        mongo.update_order_status(order['address'], 'completed', 'Completed order.')
                    elif res == 1:
                        mongo.process_canceled_order(order, TIMEOUTS_ORDER[order['source_coin']])
            time.sleep(0.1)
        except Exception as e:
            print_log("check_transactions_thread:" + str(e), "ERROR", 3)
            time.sleep(0.1)


def calc_min_confirms(amount):
    return 1


def get_fee_level(tx):
    fee = tx['gas'] * tx['gas_price']
    ether = my_constant.ETH_WEB3.fromWei(fee, 'ether')
    try:
        safe_gas = get_gas_price('safeLow')
        if safe_gas >= tx['gas_price']:
            fee = 'low ' + str(ether)
        else:
            fee = 'high ' + str(ether)
        return fee
    except Exception as e:
        print_log('eth:get_fee_level:' + str(e), 'ERROR', 3)
        return 'low ' + str(ether)


def make_transaction(tx, btc_miner_fee=0, ex_price=0):
    try:
        fee = get_fee_level(tx)# need to calculate fee level
        confirm = int(get_confirmations(tx))
        min_conf = calc_min_confirms(tx['amount'])
        if confirm >= min_conf:
            status = "created"
        else:
            status = "created"

        data = {'address': str(tx['address']), 'amount': tx['amount'], 'refunded': 0.0, 'confirming_time': '',
                'confirmations': confirm, 'tx_id': tx['tx']['hash'].hex(), 'min_conf': min_conf, 'sent': 0,
                'time': datetime.now(), 'miner_fee': fee, 'processing_time': '',
                'step': 0, 'status': status, 'received': 0.0, 'cold_index': -1, 'cold_token_tx': '', 'cold_eth_tx': '',
                'comment': '', 'dst_amt': 0.0, 'took_time': 0, 'tx_cw': {'amount': 0, 'address': '', 'tx_id': ''},
                'tx': [], 'tx_ref': [], 'dest_miner_fee': my_bitcoin.get_dest_miner_fee(tx['amount'], ex_price, btc_miner_fee)}
        return data
    except Exception as e:
        print_log("make_transaction:" + str(e), "ERROR", 3)
        return None


def get_confirmations(tx):
    end_block = int(my_constant.ETH_WEB3.eth.blockNumber)
    return end_block - int(tx['tx']['blockNumber'])


def confirm_payment(order, tx):
    while True:
        try:
            if mongo.is_existed_out_tx_order(order['address'], tx) is False:
                return None
            result = {'amount': 0.0, 'cold_index': -1, 'cold_token_tx': '', 'cold_eth_tx': ''}
            conf = my_constant.ETH_WEB3.eth.getTransactionReceipt(tx['tx_id'])
            if conf is None:
                return result
            if conf['blockNumber'] <= 0:
                return result
            if conf['status'] == 0:
                return result
            if get_token_balance(order['address'], order['source_coin']) <= 0:
                return result
            end_block = int(my_constant.ETH_WEB3.eth.blockNumber)
            confirm = end_block - int(conf['blockNumber'])
            # tx['confirmations'] = confirm
            min_conf = calc_min_confirms(tx['amount'])
            if confirm < min_conf:
                time.sleep(1)
                continue
            index = find_min_token_wallet(order['source_coin'])
            result['cold_index'] = index
            result['amount'] = tx['amount']
            account = get_account_from_private_key(index)
            if account is None:
                print_log("ethereum:confirm_payment: get destination account failed", "ERROR", 3)
                return result
            eth_limit = get_eth_limit(order['gas_price'])
            if order['source_coin'] != 'ETH' and get_eth_balance(order['address']) < eth_limit:
                eth_dst = eth_limit * 2
                tx_ref = []
                partial_transfer_eth(order['address'], order['address'], eth_dst, tx, 'ETH', order['gas_price'], tx_ref)
                if len(tx_ref) == 0:
                    print_log("ethereum:confirm_payment: transfer eth failed", "ERROR", 3)
                    return result  # warning
                print_log("ethereum:confirm_payment: transfer eth success", "ALARM", 3)

                while True:
                    eth_balance = get_eth_balance(order['address'])
                    if eth_balance >= eth_limit:
                        break
                    print_log("ethereum:confirm_payment: transfer eth waiting to confirm", "ALARM", 3)
                    time.sleep(1)

                while True:
                    trans = transfer_token(order['address'], HexBytes(order['private']), account.address, tx['amount'], ETH_GAS_LIMIT, order['gas_price'], order['source_coin'], True)
                    if trans['code'] == 0:
                        result['cold_token_tx'] = trans['tx']
                        break
                    print_log("ethereum:confirm_payment: transfer token failed, message:" + trans['message'], "ERROR", 3)
                    time.sleep(1)
                print_log("ethereum:confirm_payment: transfer token success", "ALARM", 3)
            while True:
                print_log("ethereum:confirm_payment: transfer gas process", "ALARM", 3)
                dst_amt = calc_exact_eth_dst_amount(order['address'], order['gas_price'])
                if dst_amt <= 0:
                    print_log("ethereum:confirm_payment: already transferred gas", "ALARM", 3)
                    break
                trans = transfer_eth(order['address'], HexBytes(order['private']), account.address, dst_amt, ETH_GAS_MIN_LIMIT, order['gas_price'], True)
                if trans['code'] == 0:
                    result['cold_eth_tx'] = trans['tx']
                    print_log("ethereum:confirm_payment: transfer gas success:" + str(trans['tx']), "ALARM", 3)
                    break
                print_log("ethereum:confirm_payment: transfer eth failed, message:" + trans['message'], "ERROR", 3)
                time.sleep(1)
            print_log("ethereum:confirm_payment: final confirmed", "ALARM", 3)
            return result
        except Exception as e:
            print_log("ethereum:confirm_payment:" + str(e), "ERROR", 3)
            time.sleep(1)


def calc_exact_eth_dst_amount(address, gas_price):
    while True:
        try:
            eth_balance = my_constant.ETH_WEB3.eth.getBalance(address)
            eth_limit = ETH_GAS_MIN_LIMIT * my_constant.ETH_WEB3.toWei(gas_price, 'gwei')
            eth_dst_wei = eth_balance - eth_limit
            if eth_dst_wei <= 0:
                print_log("calc_exact_eth_dst_amount: not enough balance, eth:" + str(eth_balance) + ", gas:" + str(eth_limit), "ALARM", 3)
                return eth_dst_wei
            dst_amt = float(my_constant.ETH_WEB3.fromWei(eth_dst_wei, 'ether'))
            return dst_amt
        except Exception as e:
            print_log("calc_exact_eth_dst_amount:" + str(e), "ERROR", 3)
            time.sleep(1)


def insert_and_append_list_tx(tx_list, tx, btc_miner_fee=0, ex_price=0):
    try:
        for i in range(0, len(tx_list)):
            if tx_list[i]['tx_id'] == tx['tx']['hash'].hex():
                if tx_list[i]['confirmations'] < get_confirmations(tx):
                    tx_list[i]['confirmations'] = get_confirmations(tx)
                return tx_list[i]
        order = make_transaction(tx, btc_miner_fee, ex_price)
        if order is None:
            return None
        tx_list.append(order)
        return order
    except Exception as e:
        print_log("insert_and_append_list_tx:" + str(e), "ERROR", 3)
        return None


def update_order_amounts(order):
    try:
        received = 0.0
        refunded = 0.0
        dst_amt = 0.0
        completed = True
        received_usd = 0.0
        for item in order['out_tx_list']:
            if item['step'] == 0 and find_item_in_tx_list(item) is False:
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
        mongo.update_order_amount(order['address'], order['src_amt'], floating(dst_amt, 8),
                                  floating(received, get_decimals_token(order['source_coin'])),
                                  floating(refunded, get_decimals_token(order['source_coin'])),
                                  floating(received_usd, 2))
        if received == 0:
            return 1  # cancelling
        if completed is True:
            return 0
        return -1
    except Exception as e:
        print_log("eth:update_order_amounts:" + str(e), "ERROR", 3)
        return -2


def find_item_in_tx_list(item):
    try:
        trans = my_constant.ETH_WEB3.eth.getTransactionReceipt(item['tx_id'])
        if trans is None:
            return False
        if trans['status'] == 0:
            return False
        return True
    except Exception as e:
        print_log("find_item_in_tx_list:" + str(e), "ERROR", 3)
    return False


def refund_payment(address, dst_address, dst_amt, order_tx, token, gas_price, tx_ref):
    while True:
        try:
            locked = lock_outputs(dst_amt, order_tx, address, token, gas_price)
            if locked is not None:
                break
            print_log(token + ":refund_payment:lock_outputs is None", 'ERROR', 5)
            time.sleep(1)
        except Exception as e:
            print_log(token + ":refund_payment:lock_outputs" + str(e), 'WARNING', 3)
    step = 0
    for item in locked:
        while True:
            try:
                # time.sleep(3)
                print_log(token + ":refund_payment: from outputs(index:" + str(item['index']) + ") amount:"
                          + str(item['amount']) + " address:" + address, "NORMAL", 5)
                source_account = get_account_from_private_key(item['index'])
                if source_account is None:
                    print_log(token + ":refund_payment: failed, not get source address private key", "ERROR", 3)
                    time.sleep(3)
                    continue
                if gas_price == 0:
                    gas_price = item['eth_limit'] * pow(10, 9) / ETH_GAS_LIMIT
                if token == 'ETH':
                    tx_hash = transfer_eth(source_account.address, source_account.privateKey, dst_address,
                                             item['amount'], ETH_GAS_LIMIT, gas_price, True)
                else:
                    tx_hash = transfer_token(source_account.address, source_account.privateKey, dst_address,
                                             item['amount'], ETH_GAS_LIMIT, gas_price, token, True)
                if tx_hash['code'] == 0:
                    unlock_outputs(item, order_tx, address, token)
                    tx_ref.append(tx_hash['tx'])
                    break
                elif tx_hash['code'] > -3:
                    print_log(token + ":refund_payment:" + str(tx_hash['message']) + " from outputs(index:" + str(item['index']) + ") amount:" + str(item['amount']) + " address:" + address, "ERROR", 3)
                    if tx_hash['message'].find('Could not find address') >= 0:
                        unlock_outputs(item, order_tx, address, token)
                        step = 3
                        break
                    else:
                        details = "Queue, because our balance is locked."
                        time.sleep(3)
                elif tx_hash['code'] > -10:
                    print_log(token + ":refund_payment critical:" + str(tx_hash['message']) + " from outputs(index:" +
                              str(item['index']) + ") amount:" + str(item['amount']) + " address:" + address, "ERROR",
                              3)
                    unlock_outputs(item, order_tx, address, token)
                    refund_payment(address, dst_address, item['amount'], order_tx, token, gas_price, tx_ref)
                    break
            except Exception as err:
                print_log(token + ":refund_payment:" + str(err), "ERROR", 3)
                time.sleep(3)
        if step == 3:
            break
    unlock_all_outputs(locked, order_tx, address, token)


def partial_transfer_eth(address, dst_address, dst_amt, order_tx, token, gas_price, tx_ref):
    while True:
        locked = lock_outputs(dst_amt, order_tx, address, token, gas_price)
        if locked is not None:
            break
        print_log(
            'eth:partial_transfer_eth:transfer eth locked is none, address:' + address + ' tx_id:' + order_tx['tx_id'],
            'ERROR', 3)
        time.sleep(1)
    step = 0
    for item in locked:
        while True:
            try:
                # time.sleep(3)
                print_log(token + ":partial_transfer_eth: from outputs(index:" + str(item['index']) + ") amount:"
                          + str(item['amount']) + " address:" + address, "NORMAL", 5)
                source_account = get_account_from_private_key(item['index'])
                if source_account is None:
                    print_log(token + ":partial_transfer_eth: failed, not get source address private key", "ERROR", 3)
                    time.sleep(3)
                    continue
                if gas_price == 0:
                    gas_price = item['eth_limit'] * pow(10, 9) / ETH_GAS_LIMIT
                tx_hash = transfer_eth(source_account.address, source_account.privateKey, dst_address,
                                         item['amount'], ETH_GAS_LIMIT, gas_price, True)
                if tx_hash['code'] == 0:
                    unlock_outputs(item, order_tx, address, token)
                    tx_ref.append(tx_hash['tx'])
                    break
                elif tx_hash['code'] > -3:
                    print_log(token + ":partial_transfer_eth:" + str(tx_hash['message']) + " from outputs(index:" + str(item['index']) + ") amount:" + str(item['amount']) + " address:" + address, "ERROR", 3)
                    if tx_hash['message'].find('Could not find address') >= 0:
                        unlock_outputs(item, order_tx, address, token)
                        step = 3
                        break
                    else:
                        details = "Queue, because our balance is locked."
                        time.sleep(3)
                elif tx_hash['code'] > -10:
                    print_log(token + ":partial_transfer_eth critical:" + str(tx_hash['message']) + " from outputs(index:" +
                              str(item['index']) + ") amount:" + str(item['amount']) + " address:" + address, "ERROR",
                              3)
                    unlock_outputs(item, order_tx, address, token)
                    partial_transfer_eth(address, dst_address, item['amount'], order_tx, token, gas_price, tx_ref)
                    break
            except Exception as err:
                print_log(token + ":partial_transfer_eth:" + str(err), "ERROR", 3)
                time.sleep(3)
        if step == 3:
            break
    unlock_all_outputs(locked, order_tx, address, token)

