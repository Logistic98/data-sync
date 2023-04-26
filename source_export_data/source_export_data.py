# -*- coding: utf-8 -*-

import os
import sys
import time
from datetime import datetime
from decimal import Decimal
from configparser import ConfigParser
import schedule

import gol
from log import logger
from export_es_data import export_es_data_main
from export_mysql_data import export_mysql_data_main
from utils import dict_to_json_file, read_json_to_dict, encrypt_data_file, zip_data_dir, get_now_time


# 读取配置文件
def read_config(config_path):
    cfg = ConfigParser()
    cfg.read(config_path, encoding='utf-8')
    section_list = cfg.sections()
    config_dict = {}
    for section in section_list:
        section_item = cfg.items(section)
        for item in section_item:
            config_dict[item[0]] = item[1]
    return config_dict


# 目录路径不存在时自动创建
def path_not_exist_auto_create(file_path, logger_info):
    if not os.path.exists(file_path):
        os.makedirs(file_path)
        logger.info(logger_info)


# 导出数据主任务
def source_export_data_main_job():

    is_running = gol.get_value('is_running')
    if not is_running:
        gol.set_value('is_running', True)

        # 获取任务开始时间
        start_time = time.time()
        start_time_str1, start_time_str2 = get_now_time()  # start_time_str1用来给文件夹命名，start_time_str2作为时间筛选条件
        logger.info("----------开始导出源数据----------")

        # 读取上次任务的同步时间（如果该时间为空字符串，则跑全量）
        last_job_time = ""
        last_job_time_dict = {}
        try:
            last_job_time_dict = read_json_to_dict(last_job_time_path)
            last_job_time = last_job_time_dict['last_job_time']
        except Exception as e:
            logger.error("读取上次同步时间文件出错：{}".format(e))

        # 数据存储目录
        original_data_base_path = str(source_export_dict['original_data_base_path'])
        encrypt_data_base_path = str(source_export_dict['encrypt_data_base_path'])
        data_package_base_path = str(source_export_dict['data_package_base_path'])
        start_time_str = "init"
        if last_job_time != "":
            try:
                start_time_str = datetime.strptime(last_job_time, "%Y-%m-%d %H:%M:%S").strftime("%Y_%m_%d_%H_%M_%S")
            except Exception as e:
                logger.error("格式化上次更新时间出错：{}".format(e))
        original_data_path = "{}/{}/{}--{}".format(base_path, original_data_base_path, start_time_str, start_time_str1)
        encrypt_data_path = "{}/{}/{}--{}".format(base_path, encrypt_data_base_path, start_time_str, start_time_str1)
        data_package_path = "{}/{}/{}--{}.zip".format(base_path, data_package_base_path, start_time_str, start_time_str1)

        # 执行数据导出任务
        path_not_exist_auto_create(original_data_path, "已创建原始数据文件路径{}".format(original_data_path))
        es_is_open = source_export_dict['es_is_open']
        if es_is_open == "true":
            try:
                logger.info("---开始导出ES源数据")
                export_es_data_main(source_export_dict, original_data_path, last_job_time, start_time_str2)
                logger.info("---导出ES源数据已完成")
            except Exception as e:
                logger.error("导出ES源数据出错：{}".format(e))
        mysql_is_open = source_export_dict['mysql_is_open']
        if mysql_is_open == "true":
            try:
                logger.info("---开始导出MySQL源数据")
                export_mysql_data_main(source_export_dict, original_data_path, last_job_time, start_time_str2)
                logger.info("---导出MySQL源数据已完成")
            except Exception as e:
                logger.error("导出MySQL源数据出错：{}".format(e))

        # 加密数据
        path_not_exist_auto_create(encrypt_data_path, "已创建加密数据文件路径{}".format(original_data_path))
        try:
            logger.info("---开始加密所有源数据")
            encrypt_data_file(original_data_path, encrypt_data_path, public_rsa_key_path)
            logger.info("---加密所有源数据已完成")
        except Exception as e:
            logger.error("加密数据出错：{}".format(e))

        # 压缩数据
        try:
            logger.info("---开始压缩所有加密后的数据")
            zip_data_dir(encrypt_data_path, data_package_path)
            logger.info("---压缩所有加密后的数据已完成")
            logger.info("加密压缩后的数据包文件路径为{}".format(data_package_path))
        except Exception as e:
            logger.error("压缩数据出错：{}".format(e))

        # 更新本次任务的时间
        try:
            last_job_time_dict['last_job_time'] = start_time_str2
            dict_to_json_file(last_job_time_dict, last_job_time_path)
            logger.info("更新本次任务的时间{}写入文件{}".format(start_time_str2, last_job_time_path))
        except Exception as e:
            logger.error("更新本次任务的时间写入文件出错：{}".format(e))

        # 获取任务结束时间并统计耗时
        end_time = time.time()
        time_consuming_str = str(Decimal(str((end_time - start_time) * 1000)).quantize(Decimal('0.00'))) + 'ms'
        logger.info("----------导出源数据已完成，共耗时：{}----------".format(time_consuming_str))

        gol.set_value('is_running', False)
    else:
        logger.warn("当前存在正在执行中的数据导出任务，本次暂不执行")


if __name__ == '__main__':

    logger.info("==========启动导出数据程序==========")

    # 获取基础路径并读取配置信息
    base_path = os.getcwd()
    logger.info("基础路径：{}".format(base_path))

    config_path = '{}/config.ini'.format(base_path)
    logger.info("配置文件路径：{}".format(config_path))
    source_export_dict = {}
    try:
        source_export_dict = read_config(config_path)
    except:
        logger.error("读取配置文件出错，程序已终止执行")
        sys.exit()

    public_rsa_key_path = "{}/{}".format(base_path, str(source_export_dict['public_rsa_key_path']))
    logger.info("RSA公钥文件路径：{}".format(public_rsa_key_path))
    if not os.path.exists(public_rsa_key_path):
        logger.error("RSA公钥文件不存在，程序已终止执行")
        sys.exit()

    last_job_time_path = "{}/{}".format(base_path, str(source_export_dict['last_job_time_path']))
    logger.info("上次任务同步时间记录文件路径：{}".format(last_job_time_path))
    if not os.path.exists(last_job_time_path):
        last_job_time_dict = {"last_job_time": ""}
        dict_to_json_file(last_job_time_dict, last_job_time_path)
        logger.info("上次任务同步时间记录文件不存在，已自动创建")

    original_data_base_path = "{}/{}".format(base_path, str(source_export_dict['original_data_base_path']))
    logger.info("原始数据文件根路径：{}".format(original_data_base_path))
    path_not_exist_auto_create(original_data_base_path, "原始数据文件根路径不存在，已自动创建")

    encrypt_data_base_path = "{}/{}".format(base_path, str(source_export_dict['encrypt_data_base_path']))
    logger.info("加密后的数据文件根路径：{}".format(encrypt_data_base_path))
    path_not_exist_auto_create(encrypt_data_base_path, "加密后的数据文件根路径不存在，已自动创建")

    data_package_base_path = "{}/{}".format(base_path, str(source_export_dict['data_package_base_path']))
    logger.info("加密压缩后的数据包文件根路径：{}".format(data_package_base_path))
    path_not_exist_auto_create(data_package_base_path, "加密压缩后的数据包文件路径根不存在，已自动创建")

    # 配置定时任务，可同时启动多个
    logger.info("定时任务规则：{}".format("每隔30分钟运行一次job"))
    schedule.every(30).minutes.do(source_export_data_main_job)

    logger.info("==========加载配置完成==========")

    gol._init()
    gol.set_value('is_running', False)   # 设置is_running状态，避免上一次没跑完又开始了下一次的任务，出现数据重复同步问题

    # 运行所有可以运行的任务
    logger.info("运行所有可以运行的定时任务")
    source_export_data_main_job()        # 启动时立即执行一次，随后进入定时任务
    while True:
        schedule.run_pending()
        time.sleep(1)
