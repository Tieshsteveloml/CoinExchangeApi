from config.config import *
from datetime import datetime
import os
import constant as my_constant
from zipfile import ZipFile, ZIP_DEFLATED


def print_log(message, caption="DEBUG", log_level=1):
    try:
        if log_level < LOG_LEVEL:
            return
        buffer = str(message)
        log = "[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] " + caption + ": " + buffer + "\n"
        if PRINT_ENABLE:
            print(log)
        if LOG_ENABLE is False:
            return
        orig = my_constant.ROOT_PATH + "/" + LOG_FILE_PATH
        if os.path.exists(orig):
            size = os.path.getsize(orig)
            if size > LOG_MAX_SIZE * 1000:
                end = LOG_FILE_PATH.find('/')
                log_dir = LOG_FILE_PATH[0:end]
                new_name = my_constant.ROOT_PATH + "/" + log_dir + "/log_" + datetime.now().strftime("%Y-%m-%dZ%H-%M-%S")
                backup = new_name + ".txt"
                zip_name = new_name + '.zip'
                try:
                    os.rename(orig, backup)
                    with ZipFile(zip_name, 'w', compression=ZIP_DEFLATED) as zip:
                        zip.write(backup)
                    os.remove(backup)
                except Exception as e:
                    print("Log Rename Error:" + str(e))
        file = open(orig, 'a')
        file.writelines(log)
        file.close()
    except Exception as e:
        print(str(e))


def track_begin(request):
    try:
        remote = request.remote_addr
        path = request.full_path
        method = request.method
        message = "BEGIN REMOTE_ADDR:" + remote + " FULL_PATH:" + path + " METHOD:" + method
        print_log(message, "TRACK", 4)
    except Exception as e:
        print(str(e))


def track_end(request, result):
    try:
        remote = request.remote_addr
        path = request.full_path
        method = request.method
        message = "END REMOTE_ADDR:" + remote + " FULL_PATH:" + path + " METHOD:" + method + " RESULT:" + str(result)
        print_log(message, "TRACK", 4)
    except Exception as e:
        print(str(e))


def read_log():
    try:
        orig = my_constant.ROOT_PATH + "/" + LOG_FILE_PATH
        with open(orig) as file:
            content = file.readlines()
        file.close()
        content = [x.strip() for x in content]
        read_data = []
        for i in range(len(content) - 1, -1, -1):
            read_data.append(content[i])
        return read_data
    except Exception as e:
        print(str(e))
        return str(e)


def delete_log():
    try:
        orig = my_constant.ROOT_PATH + "/" + LOG_FILE_PATH
        if os.path.exists(orig):
            os.remove(orig)
    except Exception as e:
        print(str(e))
