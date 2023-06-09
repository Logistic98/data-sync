# -*- coding: utf-8 -*-

import os
import shutil
from configparser import ConfigParser

from utils import dict_to_json_file

"""
作用：删除所有数据文件、日志并归零上次任务的同步时间
"""


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


# 若目录存在，则递归删除目录
def shutil_rmtree(path):
    if os.path.exists(path):
        shutil.rmtree(path)


if __name__ == '__main__':

    # 读取配置文件
    base_path = os.getcwd()
    config_path = '{}/config.ini'.format(base_path)
    source_export_dict = read_config(config_path)

    # 递归删除数据目录
    shutil_rmtree(str(source_export_dict['original_data_base_path']))
    shutil_rmtree(str(source_export_dict['encrypt_data_base_path']))
    shutil_rmtree(str(source_export_dict['data_package_base_path']))

    # 删除日志并归零上次任务的同步时间
    last_job_time_path = str(source_export_dict['last_job_time_path'])
    if os.path.exists(last_job_time_path):
        init_dict = {}
        init_dict['last_job_time'] = ""
        dict_to_json_file(init_dict, last_job_time_path)
    log_path = "./target_import_data.log"
    if os.path.exists(log_path):
        os.remove(log_path)

    print("删除日志并归零上次任务的同步时间执行完毕")