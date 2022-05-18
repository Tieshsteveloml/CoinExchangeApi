from pymongo import MongoClient
from constant import TRANSACTION_MUTEX
from config.config import *
from logger import print_log
from datetime import datetime, timedelta


def mongo_floating(value, digit):
    form = '.' + str(digit) + 'f'
    return float(format(float(value), form))

"""
def mongo_floating(value, digit):
    value = float(value)
    if value < 0:
        return math.ceil(value * 10 ** digit) / 10 ** digit
    else:
        return math.floor(value * 10 ** digit) / 10 ** digit
"""


try:
    client = MongoClient(MONGO_DB_SERVER, MONGO_DB_PORT)
    db = client.furkan_db
    swaps_data = db.swaps_data
    reviews = client.testimonials
    reviews_swap = reviews.swaps_data
    print_log('Mongodb started')
except Exception as err:
    print_log('Mongodb Exception: ' + str(err))


def post_review(data):
    if data is None:
        return
    post_id = reviews_swap.insert_one(data)
    print_log("Mongodb(post_review): insert id:" + str(post_id.inserted_id))


def get_reviews(rate, is_approved=0):
    result = []
    try:
        if rate == 0:
            rates = reviews_swap.find({}, {'_id': False})
        else:
            rates = reviews_swap.find({'rate': rate}, {'_id': False})
        if rates is None:
            return []
        for item in rates:
            if is_approved == 1 and item['is_approved'] is False:
                continue
            if is_approved == -1 and item['is_approved'] is True:
                continue
            current_date = datetime.now()
            create_date = item['create_date']
            delta = current_date - create_date
            sec_delta = timedelta(seconds=60)
            min_delta = timedelta(minutes=60)
            hour_delta = timedelta(hours=24)
            month_delta = timedelta(days=30)
            year_delta = timedelta(days=365)
            if delta < sec_delta:
                if delta.total_seconds() > 1:
                    ago_min = str(int(delta.total_seconds())) + " seconds ago"
                else:
                    ago_min = "1 second ago"
            elif delta < min_delta:
                if int(delta.total_seconds() / 60) > 1:
                    ago_min = str(int(delta.total_seconds() / 60)) + " minutes ago"
                else:
                    ago_min = "1 minute ago"
            elif delta < hour_delta:
                if int(delta.total_seconds() / 3600) > 1:
                    ago_min = str(int(delta.total_seconds() / 3600)) + " hours ago"
                else:
                    ago_min = "1 hour ago"
            elif delta < month_delta:
                if int(delta.days) > 1:
                    ago_min = str(int(delta.days)) + " days ago"
                else:
                    ago_min = "1 day ago"
            elif delta < year_delta:
                if round(delta.days / 30) > 1:
                    ago_min = str(round(delta.days / 30)) + " months ago"
                else:
                    ago_min = "1 month ago"
            else:
                if round(delta.days / 365) > 1:
                    ago_min = str(round(delta.days / 365)) + " years ago"
                else:
                    ago_min = "1 year ago"
            create_date = create_date.strftime("%Y-%m-%d %H:%M")
            amount = format(item['amount'], '.4f')
            if item['amount'] < 1:
                amount = amount[1:]
            data = {'rate': item['rate'], 'description': item['description'],'order_type': item['order_type'], 'ago_min': ago_min,
                'source_cur': item['source_cur'],'amount': amount, 'create_date': create_date, 'is_approved': item['is_approved']}
            result.append(data)
        return result[::-1]
    except Exception as e:
        print_log("MONGODB EXCEPTION(get_reviews): " + str(e), "ERROR", 3)
        return result[::-1]


def post_to_db(swap):
    if swap is None:
        return
    post_id = swaps_data.insert_one(swap)
    print_log("Mongodb(post_to_db): insert id:" + str(post_id.inserted_id))


def find_order_by_address(address, source_coin=''):
    if source_coin == '':
        order = swaps_data.find_one({'address': address})
    else:
        order = swaps_data.find_one({'address': address, 'source_coin': source_coin})
    return order


def get_order_date(address):
    try:
        order = find_order_by_address(address)
        if order is None:
            return None
        date = order['date']
        return date
    except Exception as e:
        print_log("MONGODB EXCEPTION(get_order_date): " + str(e), "ERROR", 3)
        return None


def get_order_out_tx_list(address):
    try:
        order = find_order_by_address(address)
        if order is None:
            return []
        return order['out_tx_list']
    except Exception as e:
        print_log("MONGODB EXCEPTION(get_order_out_tx_list): " + str(e), "ERROR", 3)
        return []


def get_order_received(address):
    try:
        order = find_order_by_address(address)
        if order is None:
            return 0
        return order['received']
    except Exception as e:
        print_log("MONGODB EXCEPTION(get_order_status): " + str(e), "ERROR", 3)
        return 0


def get_order_status(address):
    try:
        order = find_order_by_address(address)
        if order is None:
            return None
        return order['status']
    except Exception as e:
        print_log("MONGODB EXCEPTION(get_order_status): " + str(e), "ERROR", 3)
        return ''


def get_order_btc_usd_rate(address):
    try:
        order = find_order_by_address(address)
        if order is None:
            return None
        return order['btc_usd']
    except Exception as e:
        print_log("MONGODB EXCEPTION(get_order_btc_usd_rate): " + str(e), "ERROR", 3)
        return ''


def update_out_tx_list(address, out_tx_list):
    try:
        query = {'address': address}
        new_values = {"$set": {"out_tx_list": out_tx_list}}
        result = swaps_data.update_many(query, new_values)
        print_log("MONGODB DEBUG(update_out_tx_list): " + str(result.raw_result))
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_out_tx_list): " + str(e), "ERROR", 3)
        return 0


def pull_order(address):
    result = []
    try:
        order = find_order_by_address(address)
        if order is None:
            return []
        if order['status'] == 'created':
            rest_second = calc_order_rest_time_out(order)
        else:
            rest_second = 0
        print_log("pull_order: rest_second:" + str(rest_second), 'DEBUG', 2)
        data = {'address': order['address'], 'status': order['status'], 'order_type': order['order_type'],
                'details': order['details'], 'out_tx_list': order['out_tx_list'],
                'refunded_amt': order['refunded_amt'], 'src_amt': order['src_amt'],
                'received': order['received'], 'dst_amt': order['dst_amt'],
                'exceed': order['exceed'], 'underpay': order['underpay'], 'end_date': order['end_date'],
                'lack': order['lack'], 'btc_miner_fee': order['btc_miner_fee'],
                'dest_coin': order['dest_coin'], 'rest_second': rest_second, 'ex_price': order['ex_price'],
                'gas_price': order['gas_price'], 'gas_reduce': order['gas_reduce'],
                'source_coin': order['source_coin']}
        result.append(data)
        return result
    except Exception as e:
        print_log("MONGODB EXCEPTION(pull_order): " + str(e), "ERROR", 3)
    return result


def update_received_usd(address, received_usd):
    try:
        query = {'address': address}
        new_values = {"$set": {"received_usd": received_usd}}
        result = swaps_data.update_many(query, new_values)
        print_log("MONGODB DEBUG(update_received_usd): " + str(result.raw_result))
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_received_usd): " + str(e), "ERROR", 3)


def update_refunded_amount(address, refunded):
    try:
        query = {'address': address}
        new_values = {"$set": {"refunded_amt": refunded}}
        result = swaps_data.update_many(query, new_values)
        print_log("MONGODB DEBUG(update_refunded_amount): " + str(result.raw_result))
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_refunded_amount): " + str(e), "ERROR", 3)


def update_btc_usd_rate(address, rate):
    try:
        query = {'address': address}
        new_values = {"$set": {"btc_usd": rate}}
        result = swaps_data.update_many(query, new_values)
        print_log("MONGODB DEBUG(update_btc_usd_rate): " + str(result.raw_result))
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_btc_usd_rate): " + str(e), "ERROR", 3)


def update_order_amount(address, amount, dst_amt, received, refunded, received_usd=0.0):
    try:
        if amount > received:
            underpay = True
            exceed = False
            lack = mongo_floating(amount - received, 8)
        elif amount < received:
            underpay = False
            exceed = True
            lack = mongo_floating(received - amount, 8)
        else:
            underpay = False
            exceed = False
            lack = 0.0
        my_query = {'address': address}
        new_values = {"$set": {"src_amt": amount, "dst_amt": dst_amt, "received": received,
                               "received_usd": received_usd, "refunded_amt": refunded,
                               "underpay": underpay, "exceed": exceed, "lack": lack}}
        result = swaps_data.update_many(my_query, new_values)
        print_log("MONGODB DEBUG(update_order_amount): " + str(result.raw_result))
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_order_amount): " + str(e), "ERROR", 3)


def is_existed_out_tx_order(address, out_tx):
    try:
        TRANSACTION_MUTEX.acquire()
        out_list = get_order_out_tx_list(address)
        found = False
        for i in range(0, len(out_list)):
            if out_list[i]['tx_id'] == out_tx['tx_id'] and out_list[i]['step'] != 8:
                found = True
                break
        TRANSACTION_MUTEX.release()
        return found
    except Exception as e:
        print_log("MONGODB EXCEPTION(is_existed_out_tx_order): " + str(e), "ERROR", 3)
        if TRANSACTION_MUTEX.locked():
            TRANSACTION_MUTEX.release()
        return False


def update_tx(address, out_tx):
    try:
        TRANSACTION_MUTEX.acquire()
        out_list = get_order_out_tx_list(address)
        for i in range(0, len(out_list)):
            if out_list[i]['tx_id'] == out_tx['tx_id']:
                out_list[i] = out_tx
                break
        update_out_tx_list(address, out_list)
        TRANSACTION_MUTEX.release()
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_tx): " + str(e), "ERROR", 3)
        if TRANSACTION_MUTEX.locked():
            TRANSACTION_MUTEX.release()


def delete_tx(address, out_tx):
    try:
        TRANSACTION_MUTEX.acquire()
        out_list = get_order_out_tx_list(address)
        new_out_tx = []
        for i in range(0, len(out_list)):
            if out_list[i]['tx_id'] == out_tx['tx_id']:
                continue
            new_out_tx.append(out_list[i])
        update_out_tx_list(address, new_out_tx)
        TRANSACTION_MUTEX.release()
    except Exception as e:
        print_log("MONGODB EXCEPTION(delete_tx): " + str(e), "ERROR", 3)
        if TRANSACTION_MUTEX.locked():
            TRANSACTION_MUTEX.release()


def update_bad_tx(address, out_tx):
    try:
        TRANSACTION_MUTEX.acquire()
        out_list = get_order_out_tx_list(address)
        for i in range(0, len(out_list)):
            if out_list[i]['tx_id'] == out_tx['tx_id']:
                out_list[i]['amount'] = 0
                out_list[i]['dst_amt'] = 0
                out_list[i]['comment'] = 'Deleted or Spent TX'
                out_list[i]['status'] = 'canceled'
                out_list[i]['step'] = 8
                continue
        update_out_tx_list(address, out_list)
        TRANSACTION_MUTEX.release()
    except Exception as e:
        print_log("MONGODB EXCEPTION(delete_tx): " + str(e), "ERROR", 3)
        if TRANSACTION_MUTEX.locked():
            TRANSACTION_MUTEX.release()


def update_tx_step(address, out_tx, step, refunded=0.0):
    try:
        TRANSACTION_MUTEX.acquire()
        out_list = get_order_out_tx_list(address)
        for i in range(0, len(out_list)):
            if out_list[i]['tx_id'] == out_tx['tx_id']:
                out_list[i] = out_tx
                out_list[i]['step'] = step
                out_list[i]['refunded'] = refunded
                break
        update_out_tx_list(address, out_list)
        TRANSACTION_MUTEX.release()
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_tx_step): " + str(e), "ERROR", 3)
        if TRANSACTION_MUTEX.locked():
            TRANSACTION_MUTEX.release()


def seconds_to_hhmmss(second):
    try:
        hours = int(second / 3600)
        minutes = int(int(second - 3600 * hours) / 60)
        seconds = int(second - 3600 * hours - 60 * minutes)
        hhmmss = str(hours).zfill(2) + ":" + str(minutes).zfill(2) + ":" + str(seconds).zfill(2)
        return hhmmss
    except Exception as e:
        print_log("MONGODB EXCEPTION(seconds_to_hhmmss): " + str(e), "ERROR", 3)
        return ''


def update_transaction_status(address, out_tx, status, comment="", took=False):
    try:
        TRANSACTION_MUTEX.acquire()
        ret = 0
        out_list = get_order_out_tx_list(address)
        found = False
        for i in range(0, len(out_list)):
            if out_list[i]['tx_id'] == out_tx['tx_id']:
                out_list[i] = out_tx
                out_list[i]['status'] = status
                if status == "completed":
                    processing_time = datetime.now() - out_list[i]['time']
                    print_log("update_transaction_status: total_time:" + str(processing_time.total_seconds()) + " confirm_time:" + str(out_list[i]['took_time']), "DEBUG", 2)
                    # processing_time = int(processing_time.total_seconds() - out_list[i]['took_time'])
                    processing_time = int(processing_time.total_seconds())
                    if processing_time < 0:
                        processing_time = 0
                    out_list[i]['processing_time'] = seconds_to_hhmmss(processing_time)
                if comment != "":
                    out_list[i]['comment'] = comment
                if took:
                    duration = datetime.now() - out_list[i]['time']
                    ret = duration.total_seconds()
                    out_list[i]['took_time'] = ret
                    out_list[i]['confirming_time'] = seconds_to_hhmmss(ret)
                found = True
                break
        update_out_tx_list(address, out_list)
        TRANSACTION_MUTEX.release()
        if found is False:
            print_log("MONGODB EXCEPTION(update_transaction_status): transaction not found", "ERROR", 3)
            return -1
        return ret
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_transaction_status): " + str(e), "ERROR", 3)
        if TRANSACTION_MUTEX.locked():
            TRANSACTION_MUTEX.release()
        return 0


def update_order_status(address, status, details):
    try:
        my_query = {'address': address}
        new_values = {"$set": {"status": status, "details": details, "end_date": datetime.now()}}
        result = swaps_data.update_many(my_query, new_values)
        print_log("MONGODB DEBUG(update_order_status): " + str(result.raw_result))
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_order_status): " + str(e), "ERROR", 3)


def update_order_begin_block(address, begin_block):
    try:
        my_query = {'address': address}
        new_values = {"$set": {"begin_block": begin_block}}
        result = swaps_data.update_many(my_query, new_values)
        print_log("MONGODB DEBUG(update_order_begin_block): " + str(result.raw_result))
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_order_begin_block): " + str(e), "ERROR", 3)


def get_all_orders():
    orders = swaps_data.find({}, {'_id': False})
    result = []
    for order in orders:
        result.append(order)
    return result


def pull_by_status(status, source_coin='', dest_coin=''):
    if source_coin == '' and dest_coin == '':
        orders = swaps_data.find({'status': status}, {'_id': False})
    elif source_coin != '' and dest_coin == '':
        orders = swaps_data.find({'status': status, 'source_coin': source_coin}, {'_id': False})
    elif source_coin == '' and dest_coin != '':
        orders = swaps_data.find({'status': status, 'dest_coin': dest_coin}, {'_id': False})
    elif source_coin != '' and dest_coin != '':
        orders = swaps_data.find({'status': status, 'source_coin': source_coin, 'dest_coin': dest_coin}, {'_id': False})
    else:
        orders = swaps_data.find({'status': status}, {'_id': False})
    result = []
    for order in orders:
        result.append(order)
    return result


def pull_by_date(start_date, status, source_coin=''):
    if source_coin == '':
        orders = swaps_data.find({}, {'_id': False})
    else:
        orders = swaps_data.find({'source_coin': source_coin}, {'_id': False})
    result = []
    for index in range(orders.count() - 1, -1, -1):
        if orders[index]['date'] < start_date:
            break
        if orders[index]['status'] != status:
            continue
        result.append(orders[index])
    return result


def delete_all_orders():
    try:
        result = swaps_data.delete_many({})
        print_log("MONGODB(delete_all_orders): Deleted count:" + str(result.deleted_count))
        return result.deleted_count
    except Exception as e:
        print_log("MONGODB EXCEPTION(delete_all_orders): " + str(e), "ERROR", 3)
        return 0


def delete_all_reviews():
    try:
        result = reviews_swap.delete_many({})
        print_log("MONGODB(delete_all_reviews): Deleted count:" + str(result.deleted_count))
        return result.deleted_count
    except Exception as e:
        print_log("MONGODB EXCEPTION(delete_all_reviews): " + str(e), "ERROR", 3)
        return 0


def update_btc_tx_split(address, tx_id, steps, split_tx=None, tx_cw=None):
    try:
        TRANSACTION_MUTEX.acquire()
        out_list = get_order_out_tx_list(address)
        for i in range(0, len(out_list)):
            if out_list[i]['tx_id'] == tx_id:
                out_list[i]['split']['steps'] = steps
                if split_tx is not None:
                    out_list[i]['split']['lists'].append(split_tx)
                if tx_cw is not None:
                    print_log("update_btc_tx_split: cw:" + str(tx_cw), "DEBUG", 3)
                    out_list[i]['tx_cw'] = tx_cw
                break
        update_out_tx_list(address, out_list)
        TRANSACTION_MUTEX.release()
    except Exception as e:
        print_log("MONGODB EXCEPTION(update_btc_tx_split): " + str(e), "ERROR", 3)
        if TRANSACTION_MUTEX.locked():
            TRANSACTION_MUTEX.release()


def process_canceled_order(order, timeout):
    try:
        delta = timedelta(seconds=timeout - 5)
        if order['date'] + delta <= datetime.now():
            update_order_status(order['address'], "canceled", "Order canceled by timeout.")
            print_log(order['address'] + " Order Canceled By Timeout", "NORMAL", 5)
            return 1
        return 0
    except Exception as e:
        print_log("process_canceled_order:" + str(e), "ERROR", 3)
        return 1


def calc_order_rest_time_out(order):
    try:
        delta = timedelta(seconds=TIMEOUTS_ORDER[order['source_coin']])
        time_out = order['date'] + delta
        if time_out <= datetime.now():
            return -1
        rest_time = time_out - datetime.now()
        return rest_time.total_seconds()
    except Exception as e:
        print_log("calc_order_rest_time_out:" + str(e), "ERROR", 3)
        return -1
