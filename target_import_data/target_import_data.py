# -*- coding: utf-8 -*-

import os
import shutil
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


# 使用完毕后递归删除目录
def use_after_rmtree(rmtree_path, logger_info):
    if os.path.exists(rmtree_path):
        shutil.rmtree(rmtree_path)
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

        original_data_base_path = "{}/{}".format(base_path, str(target_import_dict['original_data_base_path']))
        encrypt_data_base_path = "{}/{}".format(base_path, str(target_import_dict['encrypt_data_base_path']))
        data_package_base_path = "{}/{}".format(base_path, str(target_import_dict['data_package_base_path']))

        # 解压数据
        logger.info("加密压缩后的数据包文件路径为{}".format(data_package_base_path))
        logger.info("---开始解压所有数据")
        zip_file_list = read_dir_to_list(data_package_base_path)
        encrypt_data_path_list = []
        for zip_file in zip_file_list:
            zip_name, zip_ext = os.path.splitext(zip_file)
            zip_file_path = "{}/{}".format(data_package_base_path, zip_file)
            encrypt_data_path = "{}/{}".format(encrypt_data_base_path, zip_name)
            path_not_exist_auto_create(zip_file_path, "给{}压缩文件自动创建解压目录{}".format(zip_file_path, encrypt_data_path))
            encrypt_data_path_list.append(encrypt_data_path)
            try:
                unzip_data_dir(zip_file_path, encrypt_data_path)
                logger.info("已将{}文件解压".format(zip_file_path))
            except Exception as e:
                logger.error("{}解压数据出错：{}".format(zip_file, e))
        logger.info("---解压所有数据已完成")
        logger.info("解压后的文件根路径为{}".format(encrypt_data_base_path))

        # 解密数据
        logger.info("---开始解密所有数据文件")
        original_data_path_list = []
        for encrypt_data_path in encrypt_data_path_list:
            original_data_path = "{}/{}".format(original_data_base_path, encrypt_data_path.split("/")[-1])
            path_not_exist_auto_create(original_data_path, "给{}加密目录自动创建解密目录{}".format(encrypt_data_path, original_data_path))
            original_data_path_list.append(original_data_path)
            try:
                decrypt_file(original_data_path, encrypt_data_path, private_rsa_key_path, rsa_key)
                logger.info("已将{}文件解密".format(encrypt_data_path))
            except Exception as e:
                logger.error("{}解密数据出错：{}".format(encrypt_data_path, e))
            use_after_rmtree(encrypt_data_path, "{}目录数据解密后递归删除".format(encrypt_data_path))
        logger.info("---解密所有数据文件已完成")

        # 导入数据
        es_is_open = target_import_dict['es_is_open']
        mysql_is_open = target_import_dict['mysql_is_open']
        for original_data_path in original_data_path_list:
            logger.info("---正在导入{}路径的数据文件".format(original_data_path))
            if es_is_open == "true":
                try:
                    logger.info("开始导入ES源数据")
                    import_es_data_main(target_import_dict, original_data_path)
                    logger.info("导入ES源数据已完成")
                except Exception as e:
                    logger.error("{}路径导入ES源数据出错：{}".format(original_data_path, e))
            if mysql_is_open == "true":
                try:
                    logger.info("开始导入MySQL源数据")
                    import_mysql_data_main(target_import_dict, original_data_path)
                    logger.info("导入MySQL源数据已完成")
                except Exception as e:
                    logger.error("{}路径导入MySQL源数据出错：{}".format(original_data_path, e))
            logger.info("---导入{}路径的数据文件已完成".format(original_data_path))
            use_after_rmtree(original_data_path, "{}目录数据导入后递归删除".format(original_data_path))

        # 更新本次任务的时间
        try:
            last_job_time_dict = {}
            last_job_time_dict['last_job_time'] = start_time_str
            dict_to_json_file(last_job_time_dict, last_job_time_path)
            logger.info("更新本次任务的时间{}写入文件{}".format(start_time_str, last_job_time_path))
        except Exception as e:
            logger.error("更新本次任务的时间写入文件出错：{}".format(e))

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

    config_path = '{}/config.ini'.format(base_path)
    logger.info("配置文件路径：{}".format(config_path))
    target_import_dict = {}
    try:
        target_import_dict = read_config(config_path)
    except:
        logger.error("读取配置文件出错，程序已终止执行")
        sys.exit()

    private_rsa_key_path = "{}/{}".format(base_path, str(target_import_dict['private_rsa_key_path']))
    logger.info("RSA私钥文件路径：{}".format(private_rsa_key_path))
    if not os.path.exists(private_rsa_key_path):
        logger.error("RSA私钥文件不存在，程序已终止执行")
        sys.exit()
    rsa_key = str(target_import_dict['rsa_key'])

    last_job_time_path = "{}/{}".format(base_path, str(target_import_dict['last_job_time_path']))
    logger.info("上次任务同步时间记录文件路径：{}".format(last_job_time_path))
    if not os.path.exists(last_job_time_path):
        last_job_time_dict = {"last_job_time": ""}
        dict_to_json_file(last_job_time_dict, last_job_time_path)
        logger.info("上次任务同步时间记录文件不存在，已自动创建")

    original_data_base_path = "{}/{}".format(base_path, str(target_import_dict['original_data_base_path']))
    logger.info("原始数据文件根路径：{}".format(original_data_base_path))
    path_not_exist_auto_create(original_data_base_path, "原始数据文件根路径不存在，已自动创建")

    encrypt_data_base_path = "{}/{}".format(base_path, str(target_import_dict['encrypt_data_base_path']))
    logger.info("加密后的数据文件根路径：{}".format(encrypt_data_base_path))
    path_not_exist_auto_create(encrypt_data_base_path, "加密后的数据文件根路径不存在，已自动创建")

    data_package_base_path = "{}/{}".format(base_path, str(target_import_dict['data_package_base_path']))
    logger.info("加密压缩后的数据包文件根路径：{}".format(data_package_base_path))
    path_not_exist_auto_create(data_package_base_path, "加密压缩后的数据包文件路径根不存在，已自动创建")

    # 配置定时任务，可同时启动多个
    logger.info("定时任务规则：{}".format("每隔30分钟运行一次job"))
    schedule.every(30).minutes.do(target_import_data_main_job)

    logger.info("==========加载配置完成==========")

    gol._init()
    gol.set_value('is_running', False)  # 设置is_running状态，避免上一次没跑完又开始了下一次的任务，出现数据重复同步问题

    # 运行所有可以运行的任务
    logger.info("运行所有可以运行的定时任务")
    target_import_data_main_job()     # 启动时立即执行一次，随后进入定时任务
    while True:
        schedule.run_pending()
        time.sleep(1)
