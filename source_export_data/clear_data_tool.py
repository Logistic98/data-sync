# -*- coding: utf-8 -*-

import os
import shutil
from configparser import ConfigParser

"""
作用：删除所有数据文件、上次任务的同步时间及日志
"""


# 读取配置文件
def read_config(config_path):
    cfg = ConfigParser()
    cfg.read(config_path, encoding='utf-8')
    last_job_time_path = cfg.get('PATH', 'last_job_time_path')
    original_data_base_path = cfg.get('PATH', 'original_data_base_path')
    encrypt_data_base_path = cfg.get('PATH', 'encrypt_data_base_path')
    data_package_base_path = cfg.get('PATH', 'data_package_base_path')
    source_export_dict = {}
    source_export_dict['last_job_time_path'] = last_job_time_path
    source_export_dict['original_data_base_path'] = original_data_base_path
    source_export_dict['encrypt_data_base_path'] = encrypt_data_base_path
    source_export_dict['data_package_base_path'] = data_package_base_path
    return source_export_dict


# 若目录存在，则递归删除目录
def shutil_rmtree(path):
    if os.path.exists(path):
        shutil.rmtree(path)


if __name__ == '__main__':

    # 读取配置文件
    base_path = os.getcwd()
    config_path = base_path + '/config.ini'
    source_export_dict = read_config(config_path)

    # 递归删除数据目录
    shutil_rmtree(str(source_export_dict['original_data_base_path']))
    shutil_rmtree(str(source_export_dict['encrypt_data_base_path']))
    shutil_rmtree(str(source_export_dict['data_package_base_path']))

    # 删除上次任务的同步时间及日志
    last_job_time_path = str(source_export_dict['last_job_time_path'])
    if os.path.exists(last_job_time_path):
        os.remove(last_job_time_path)
    log_path = "./source_export_data.log"
    if os.path.exists(log_path):
        os.remove(log_path)

    print("删除所有数据文件、上次任务的同步时间及日志完毕")