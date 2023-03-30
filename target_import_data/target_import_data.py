# -*- coding: utf-8 -*-

import os
import sys
import time
from decimal import Decimal
from configparser import ConfigParser
import schedule

import gol
from log import logger
from import_es_data import import_es_data_main
from import_mysql_data import import_mysql_data_main
from utils import dict_to_json_file, get_now_time, unzip_data_dir, decrypt_file, read_dir_to_list


# 读取配置文件
def read_config(config_path):
    cfg = ConfigParser()
    cfg.read(config_path, encoding='utf-8')
    last_job_time_path = cfg.get('PATH', 'last_job_time_path')
    original_data_base_path = cfg.get('PATH', 'original_data_base_path')
    encrypt_data_base_path = cfg.get('PATH', 'encrypt_data_base_path')
    data_package_base_path = cfg.get('PATH', 'data_package_base_path')
    rsa_key = cfg.get('RSA', 'rsa_key')
    private_rsa_key_path = cfg.get('RSA', 'private_rsa_key_path')
    es_is_open = cfg.get('TARGET_ES', 'is_open')
    es_host = cfg.get('TARGET_ES', 'host')
    es_port = cfg.get('TARGET_ES', 'port')
    es_user = cfg.get('TARGET_ES', 'user')
    es_password = cfg.get('TARGET_ES', 'password')
    es_timeout = cfg.get('TARGET_ES', 'timeout')
    es_step = cfg.get('TARGET_ES', 'step')
    mysql_is_open = cfg.get('TARGET_MYSQL', 'is_open')
    mysql_host = cfg.get('TARGET_MYSQL', 'host')
    mysql_port = cfg.get('TARGET_MYSQL', 'port')
    mysql_user = cfg.get('TARGET_MYSQL', 'user')
    mysql_password = cfg.get('TARGET_MYSQL', 'password')
    mysql_db = cfg.get('TARGET_MYSQL', 'db')
    target_import_dict = {}
    target_import_dict['last_job_time_path'] = last_job_time_path
    target_import_dict['original_data_base_path'] = original_data_base_path
    target_import_dict['encrypt_data_base_path'] = encrypt_data_base_path
    target_import_dict['data_package_base_path'] = data_package_base_path
    target_import_dict['rsa_key'] = rsa_key
    target_import_dict['private_rsa_key_path'] = private_rsa_key_path
    target_import_dict['es_is_open'] = es_is_open
    target_import_dict['es_host'] = es_host
    target_import_dict['es_port'] = es_port
    target_import_dict['es_user'] = es_user
    target_import_dict['es_password'] = es_password
    target_import_dict['es_timeout'] = es_timeout
    target_import_dict['es_step'] = es_step
    target_import_dict['mysql_is_open'] = mysql_is_open
    target_import_dict['mysql_host'] = mysql_host
    target_import_dict['mysql_port'] = mysql_port
    target_import_dict['mysql_user'] = mysql_user
    target_import_dict['mysql_password'] = mysql_password
    target_import_dict['mysql_db'] = mysql_db
    return target_import_dict


# 目录路径不存在时自动创建
def path_not_exist_auto_create(file_path, logger_info):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
        logger.info(logger_info)


# 导入数据主任务
def target_import_data_main_job():

    is_running = gol.get_value('is_running')
    if not is_running:
        gol.set_value('is_running', True)

        # 获取任务开始时间
        start_time = time.time()
        start_time_str = get_now_time()
        logger.info("----------开始导入源数据----------")

        original_data_path = base_path + "/" + str(target_import_dict['original_data_base_path'])
        encrypt_data_path = base_path + "/" + str(target_import_dict['encrypt_data_base_path'])
        data_package_path = base_path + "/" + str(target_import_dict['data_package_base_path'])

        # 解压数据
        logger.info("加密压缩后的数据包文件路径为{}".format(data_package_path))
        logger.info("---开始解压所有数据")
        zip_file_list = read_dir_to_list(data_package_path)
        for zip_file in zip_file_list:
            zip_file_path = data_package_path + "/" + zip_file
            unzip_data_dir(zip_file_path, encrypt_data_path)
            logger.info("已将{}文件解压".format(zip_file_path))
        logger.info("---解压所有数据已完成")
        logger.info("解压后的文件路径为{}".format(encrypt_data_path))

        # 解密数据
        logger.info("---开始解密所有数据文件")
        decrypt_file(original_data_path, encrypt_data_path, private_rsa_key_path, rsa_key)
        logger.info("---解密所有数据文件已完成")

        # 导入数据
        es_is_open = target_import_dict['es_is_open']
        if es_is_open == "true":
            logger.info("---开始导入ES源数据")
            import_es_data_main(target_import_dict, original_data_path)
            logger.info("---导入ES源数据已完成")
        mysql_is_open = target_import_dict['mysql_is_open']
        if mysql_is_open == "true":
            logger.info("---开始导入MySQL源数据")
            import_mysql_data_main(target_import_dict, original_data_path)
            logger.info("---导入MySQL源数据已完成")

        # 更新本次任务的时间
        last_job_time_dict = {}
        last_job_time_dict['last_job_time'] = start_time_str
        dict_to_json_file(last_job_time_dict, last_job_time_path)

        # 获取任务结束时间并统计耗时
        end_time = time.time()
        time_consuming_str = str(Decimal(str((end_time - start_time) * 1000)).quantize(Decimal('0.00'))) + 'ms'
        logger.info("----------导入源数据已完成，共耗时：{}----------".format(time_consuming_str))

        gol.set_value('is_running', False)

    else:
        logger.warn("当前存在正在执行中的数据导入任务，本次暂不执行")


if __name__ == '__main__':

    logger.info("==========启动导入数据程序==========")

    # 获取基础路径并读取配置信息
    base_path = os.getcwd()
    logger.info("基础路径：{}".format(base_path))

    config_path = base_path + '/config.ini'
    logger.info("配置文件路径：{}".format(config_path))
    target_import_dict = {}
    try:
        target_import_dict = read_config(config_path)
    except:
        logger.error("读取配置文件出错，程序已终止执行")
        sys.exit()

    private_rsa_key_path = base_path + "/" + str(target_import_dict['private_rsa_key_path'])
    logger.info("RSA私钥文件路径：{}".format(private_rsa_key_path))
    if not os.path.exists(private_rsa_key_path):
        logger.error("RSA私钥文件不存在，程序已终止执行")
        sys.exit()
    rsa_key = str(target_import_dict['rsa_key'])

    last_job_time_path = base_path + "/" + str(target_import_dict['last_job_time_path'])
    logger.info("上次任务同步时间记录文件路径：{}".format(last_job_time_path))
    if not os.path.exists(last_job_time_path):
        last_job_time_dict = {"last_job_time": ""}
        dict_to_json_file(last_job_time_dict, last_job_time_path)
        logger.info("上次任务同步时间记录文件不存在，已自动创建")

    original_data_base_path = base_path + "/" + str(target_import_dict['original_data_base_path'])
    logger.info("原始数据文件根路径：{}".format(original_data_base_path))
    path_not_exist_auto_create(original_data_base_path, "原始数据文件根路径不存在，已自动创建")

    encrypt_data_base_path = base_path + "/" + str(target_import_dict['encrypt_data_base_path'])
    logger.info("加密后的数据文件根路径：{}".format(encrypt_data_base_path))
    path_not_exist_auto_create(encrypt_data_base_path, "加密后的数据文件根路径不存在，已自动创建")

    data_package_base_path = base_path + "/" + str(target_import_dict['data_package_base_path'])
    logger.info("加密压缩后的数据包文件根路径：{}".format(data_package_base_path))
    path_not_exist_auto_create(data_package_base_path, "加密压缩后的数据包文件路径根不存在，已自动创建")

    # 配置定时任务，可同时启动多个
    logger.info("定时任务规则：{}".format("每隔30分钟运行一次job"))
    schedule.every(30).minutes.do(target_import_data_main_job)
    # logger.info("定时任务规则：{}".format("每隔1小时运行一次job"))
    # schedule.every().hour.do(target_import_data_main_job)
    # logger.info("定时任务规则：{}".format("每天在23:59时间点运行job"))
    # schedule.every().day.at("23:59").do(target_import_data_main_job)
    # logger.info("定时任务规则：{}".format("每周一运行一次job"))
    # schedule.every().monday.do(target_import_data_main_job)

    logger.info("==========加载配置完成==========")

    gol._init()
    gol.set_value('is_running', False)  # 设置is_running状态，避免上一次没跑完又开始了下一次的任务，出现数据重复同步问题

    # 运行所有可以运行的任务
    logger.info("运行所有可以运行的定时任务")
    target_import_data_main_job()     # 启动时立即执行一次，随后进入定时任务
    while True:
        schedule.run_pending()
        time.sleep(1)
