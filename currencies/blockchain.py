from selenium.webdriver.common.keys import Keys
import time
from config.config import *
import constant as my_constant
from logger import print_log
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.action_chains import ActionChains


def block_chain_login():
    options = Options()
    options.headless = True
    try:
        if my_constant.BLOCKCHAIN_DRIVER is not None:
            my_constant.BLOCKCHAIN_DRIVER.quit()
    except Exception as e:
        print_log("block_chain_login quit all windows:" + str(e), "WARNING", 3)
    my_constant.BLOCKCHAIN_DRIVER = webdriver.Firefox(options=options)
    tries = 0
    while tries < my_constant.BLOCKCHAIN_LOGIN_MAX_RETRIES:
        try:
            my_constant.BLOCKCHAIN_DRIVER.get(my_constant.BLOCKCHAIN_LOGIN_URL)
            # time.sleep(my_constant.BLOCKCHAIN_RE_LOGIN_WAIT_TIME)
            u = my_constant.BLOCKCHAIN_DRIVER.find_element_by_name(my_constant.BLOCKCHAIN_WALLET_ID_TAG_NAME)
            u.send_keys(BLOCKCHAIN_WALLET_ID)
            p = my_constant.BLOCKCHAIN_DRIVER.find_element_by_name(my_constant.BLOCKCHAIN_WALLET_PASSWORD_TAG_NAME)
            p.send_keys(BLOCKCHAIN_WALLET_PASSWORD)
            p.send_keys(Keys.RETURN)
            time.sleep(my_constant.BLOCKCHAIN_RE_LOGIN_WAIT_TIME)
            return True
        except Exception as e:
            print_log('block_chain_login:' + str(e), "WARNING", 3)
            time.sleep(my_constant.BLOCKCHAIN_RE_LOGIN_WAIT_TIME)
        tries += 1
    return False


def get_new_address_from_blockchain():
    if block_chain_login() is False:
        print_log('get_new_address_from_blockchain login failed', "WARNING", 3)
        return ''
    tries = 0
    while tries < my_constant.BLOCKCHAIN_NEW_ADDRESS_MAX_RETRIES:
        try:
            my_constant.BLOCKCHAIN_DRIVER.get(my_constant.BLOCKCHAIN_SETTINGS_URL)
            # my_constant.BLOCKCHAIN_DRIVER.refresh()
            next_button = my_constant.BLOCKCHAIN_DRIVER.find_element_by_css_selector(my_constant.BLOCKCHAIN_NEXT_BUTTON_TAG_NAME)
            next_tries = 0
            while next_tries < my_constant.BLOCKCHAIN_NEW_ADDRESS_MAX_RETRIES:
                try:
                    time.sleep(my_constant.BLOCKCHAIN_NEW_ADDRESS_WAIT_TIME)
                    new_addr = my_constant.BLOCKCHAIN_DRIVER.find_element_by_css_selector(my_constant.BLOCKCHAIN_NEXT_ADDRESS_TAG_NAME)
                    new_addr = new_addr.text
                    return new_addr
                except Exception as e:
                    print_log("get_new_address_from_blockchain creating new address:" + str(e), "WARNING", 3)
                    ActionChains(my_constant.BLOCKCHAIN_DRIVER).move_to_element(next_button).click().perform()
                next_tries += 1
            if block_chain_login() is False:
                print_log('get_new_address_from_blockchain re login failed', "WARNING", 3)
                return ''
        except Exception as e:
            print_log('get_new_address_from_blockchain:' + str(e), "WARNING", 3)
            try:
                my_constant.BLOCKCHAIN_DRIVER.find_element_by_name(my_constant.BLOCKCHAIN_WALLET_ID_TAG_NAME)
                print_log('get_new_address_from_blockchain re login begin', "WARNING", 3)
                if block_chain_login() is False:
                    print_log('get_new_address_from_blockchain re login failed', "WARNING", 3)
                    return ''
                print_log('get_new_address_from_blockchain re login end', "WARNING", 3)
            except Exception as e:
                print_log("get_new_address_from_blockchain: no re login:" + str(e), "WARNING", 3)
            time.sleep(my_constant.BLOCKCHAIN_RETRY_WAIT)
        tries += 1
    print_log('get_new_address_from_blockchain new address failed max tried', "WARNING", 3)
    return ''