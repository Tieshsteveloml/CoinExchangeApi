from config.config import *
from monero.wallet import Wallet
from monero.backends.jsonrpc import JSONRPCWallet
import time
import threading
from pycoingecko import CoinGeckoAPI
from web3 import Web3


while True:
    try:
        if USING_TEST_NET:
            XMR_WALLET = Wallet(JSONRPCWallet(port=19001, timeout=300))
        else:
            XMR_WALLET = Wallet(JSONRPCWallet(port=28088, user=RPC_XMR_USER, password=RPC_XMR_PASSWORD, timeout=300))
        break
    except Exception as e:
        print(str(e))
        time.sleep(5)

if USING_TEST_NET:
    RPC_BTC_URL = "http://%s:%s@127.0.0.1:18332" % (RPC_BTC_TEST_USER, RPC_BTC_TEST_PASSWORD)
else:
    RPC_BTC_URL = "http://%s:%s@127.0.0.1:8332" % (RPC_BTC_USER, RPC_BTC_PASSWORD)

XMR_LOCKED_BALANCE = []
for i in range(0, LOCAL_ACCOUNT_COUNT):
    data = {"tx_id": [], "locked": 0.0}
    XMR_LOCKED_BALANCE.append(data)

BTC_LOCKED_BALANCE = {}
BTC_SPLITTING_KEY = "SPLITTING_BTC"

if USING_TEST_NET:
    ETH_TOKEN_LIST = {'YFI': "0xB4FBF271143F4FBf7B91A5ded31805e42b2208d6",
                      'USDT': "0x3a9cC319b11c2dD10063EBb49aDa320A543055E2",
                      'WBTC': "0x6255F4Cdb6c08bcdA4cb7220ec71040D55e4F49f",
                      'ETH': ''}
else:
    ETH_TOKEN_LIST = {'YFI': "0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e",
                      'USDT': "0xdac17f958d2ee523a2206206994597c13d831ec7",
                      'WBTC': "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
                      'ETH': ''}


ETH_TOKEN_DECIMALS = {'YFI': {'forward': 4, 'reverse': 8},
                      'USDT': {'forward': 4, 'reverse': 8},
                      'WBTC': {'forward': 4, 'reverse': 8},
                      'ETH': {'forward': 4, 'reverse': 8}}

FRONTEND_DECIMALS = {'XMR': {'forward_price': 3, 'forward_max': 3, 'forward_amount': 2, 'reverse_price': 2, 'reverse_max': 2, 'reverse_min':2, 'reverse_amount': 4},
                     'YFI': {'forward_price': 3, 'forward_max': 3, 'forward_amount': 3, 'reverse_price': 3, 'reverse_max': 3, 'reverse_min':3, 'reverse_amount': 4},
                     'USDT': {'forward_price': 3, 'forward_max': 4, 'forward_amount': 0, 'reverse_price': 0, 'reverse_max': 0, 'reverse_min':0, 'reverse_amount': 6},
                     'WBTC': {'forward_price': 3, 'forward_max': 3, 'forward_amount': 3, 'reverse_price': 3, 'reverse_max': 3, 'reverse_min':3, 'reverse_amount': 4},
                     'ETH': {'forward_price': 3, 'forward_max': 3, 'forward_amount': 2, 'reverse_price': 2, 'reverse_max': 3, 'reverse_min':2, 'reverse_amount': 3},
                     'XVG': {'forward_price': 3, 'forward_max': 4, 'forward_amount': 0, 'reverse_price': 0, 'reverse_max': 0, 'reverse_min':0, 'reverse_amount': 6}}

ETH_LOCKED_BALANCE = []
for i in range(0, LOCAL_ACCOUNT_COUNT):
    data = {"tx_id": [], "locked": {}, "eth": 0.0}
    for j in ETH_TOKEN_LIST.keys():
        data['locked'][j] = 0.0
    ETH_LOCKED_BALANCE.append(data)

ETH_TOKENS_INFO = {}
for token in ETH_TOKEN_LIST.keys():
    data = {'btc_ex_price': {'ex_price': 0.0, 'ex_reverse': 0.0}, 'max_btc': 0.0, 'decimals': 0, 'contract': ETH_TOKEN_LIST[token]}
    ETH_TOKENS_INFO[token] = data

ETH_ACCOUNT_DICT = {}

ROOT_PATH = ""

CANCELED_ORDERS = []
THREAD_ORDERS = []
EX_BTC_USD = 0.0
POOL_NUMBER = 0
POOL_ADDRESSES = []
IS_XMR_LOAD_BALANCING = False
IS_ETH_LOAD_BALANCING = False
BTC_MUTEX = threading.Lock()
TRANSACTION_MUTEX = threading.Lock()
BTC_TRANSACTION_LIMIT = 50
XMR_OUTPUTS_MUTEX = threading.Lock()
ETH_OUTPUTS_MUTEX = threading.Lock()
BTC_OUTPUTS_MUTEX = threading.Lock()
XVG_OUTPUTS_MUTEX = threading.Lock()
UNSPEND_ONLY = True
CHECK_THREAD_MUTEX = threading.Lock()
if USING_TEST_NET:
    BLOCKSTREAM_API_URL = "https://blockstream.info/testnet/api/"
else:
    BLOCKSTREAM_API_URL = "https://blockstream.info/api/"

EX_BTC_XMR_PRICE = {'ex_price': 0.0, 'ex_reverse': 0.0}
XMR_MAX_BTC_BALANCE = 0.0
XMR_MINER_FEE = 0
COIN_GECKO_API = CoinGeckoAPI()

if USING_TEST_NET:
    ETH_WEB3 = Web3(Web3.IPCProvider('/root/.test_ethereum/geth.ipc'))
    ABI_ENDPOINT = 'https://api-goerli.etherscan.io/api?module=contract&action=getabi&address='
else:
    ETH_WEB3 = Web3()
    ABI_ENDPOINT = 'https://api.etherscan.io/api?module=contract&action=getabi&address='

ETH_CONTRACT_ABI = {}
VEY_ENDPOINT = 'https://app.stex.com/en/basic-trade/pair/BTC/VEY'
ETH_TOKEN_LOAD_BALANCING_MUTEX = threading.Lock()
ETH_GAS_URL = 'https://ethgasstation.info/api/ethgasAPI.json?api-key=9ba4f83d4b284ce6f0930ee643c3fbc5c84b5fce13eb1fad771877d06ee2'
BITCOIN_DECIMALS = 8
MONERO_DECIMALS = {'forward': 2, 'reverse': 8, 'amount': 3}
VERGE_DECIMALS = {'forward': 2, 'reverse': 8, 'amount': 8}
BITCOIN_LIMIT_UNUSED_CONFIRM = 100
BITCOIN_MAXIMUM_UNUSED_CONFIRM = 9999999
BITCOIN_CW_AMOUNT_MUTEX = threading.Lock()
BITCOIN_CW_AMOUNT_FEE = {'amount': 0.0, 'miner_fee': 0.0}
BITCOIN_SPLITTING_MINIMUM_AMOUNT = 0.00001
ETH_BEFORE_DETECT_BLOCK = 2
ETH_LIMIT_BEGIN_BLOCK = 30
ETH_LIMIT_MIN_BLOCK = 10

BLOCKCHAIN_LOGIN_URL = "https://login.blockchain.com/en/#/login"
BLOCKCHAIN_SETTINGS_URL = "https://login.blockchain.com/en/#/settings/addresses/btc/0"
BLOCKCHAIN_WALLET_ID_TAG_NAME = "guid"
BLOCKCHAIN_WALLET_PASSWORD_TAG_NAME = "password"
BLOCKCHAIN_NEXT_BUTTON_TAG_NAME = '[data-e2e="btcAddNextAddressButton"]'
BLOCKCHAIN_NEXT_ADDRESS_TAG_NAME = '[data-e2e="btcUnusedAddressLink"]'
BLOCKCHAIN_RE_LOGIN_WAIT_TIME = 5
BLOCKCHAIN_NEW_ADDRESS_WAIT_TIME = 3
BLOCKCHAIN_RETRY_WAIT = 2
BLOCKCHAIN_DRIVER = None
BLOCKCHAIN_LOGIN_MAX_RETRIES = 10
BLOCKCHAIN_NEW_ADDRESS_MAX_RETRIES = 20
BLOCKCHAIN_OVERLAY_WAIT = 20
if USING_TEST_NET:
    XMR_TESTING_ADDRESS = '9uXRFi4PZMqhsnthBF6bGdfVnBSZtfKkR7Td8qPM7jUKZeTfR1tLhCoTLqYNE12xuiQg3aWGiLw83bWsqwTRLaM4Jk47xYM'
else:
    XMR_TESTING_ADDRESS = '456U1Z74uJL6WsyDsyRram4mtDwAieDGPGQvZitKauUhatxjoz6NyhHKVA51kSMahvZrQ5HoqQj6tUM65V2K5KeXSpgL3GC'
XMR_WORKING_WALLET_INDEXES = []
XMR_WORKING_WALLET_MUTEX = threading.Lock()

EX_BTC_XVG_PRICE = {'ex_price': 0.0, 'ex_reverse': 0.0}
XVG_MAX_BTC_BALANCE = 0.0
VERGE_MAXIMUM_UNUSED_CONFIRM = 9999999
XVG_LOCKED_BALANCE = {}
XVG_MUTEX = threading.Lock()
BTC_MIN_LOAD_BALANCING_CONFIRM = 2
BTC_LOAD_BALANCING_PERIOD = 600  # seconds

XVG_MIN_LOAD_BALANCING_CONFIRM = 2
XVG_LOAD_BALANCING_PERIOD = 600  # seconds

if USING_TEST_NET:
    BTC_FEE_RATE = 0.00034
else:
    BTC_FEE_RATE = 0.00071701

