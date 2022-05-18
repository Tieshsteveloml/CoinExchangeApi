from logger import print_log
import constant as my_constant
import math


def floating(value, digit):
    form = '.' + str(digit) + 'f'
    return float(format(float(value), form))

'''
def floating(value, digit):
    value = float(value)
    if value < 0:
        return math.ceil(value * 10 ** digit) / 10 ** digit
    else:
        return math.floor(value * 10 ** digit) / 10 ** digit
'''


def is_key_dict(dict_, key):
    try:
        a = dict_[key]
        return True
    except:
        return False


def check_thread_order(tx):
    try:
        for order in my_constant.THREAD_ORDERS:
            if order == tx:
                return True
        return False
    except Exception as e:
        print_log("check_thread_order:" + str(e), "ERROR", 3)
        return False


def get_order_status(order):
    if order['status'] == 'completed':
        return 'completed'
    if order['status'] == 'canceled':
        return 'canceled'
    matches = [x for x in order['out_tx_list'] if x['step'] >= 8]
    if matches is None or len(matches) < 1:
        return 'created'
    else:
        return 'completed'