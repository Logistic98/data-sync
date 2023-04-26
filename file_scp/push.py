# -*- coding: utf-8 -*-

import os
from configparser import ConfigParser
import paramiko
import logging

'''
对于远程服务器的指定路径，可以根据筛选规则上传文件
'''

# 生成日志文件
logging.basicConfig(filename='push_file.log', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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


# 推送文件
def push(host_ip, host_port, host_username, host_password, remote_path, local_path, push_file_key, push_file_suffix):
    scp = paramiko.Transport((host_ip, host_port))
    scp.connect(username=host_username, password=host_password)
    sftp = paramiko.SFTPClient.from_transport(scp)
    files = os.listdir(local_path)
    key = push_file_key
    suffix_name = push_file_suffix
    suffix_len = len(suffix_name)
    try:
        for file in files:
            if file.find(key) != -1:
                # 判断文件后缀
                if file[-int(suffix_len):] == suffix_name:
                    local_file = local_path + file
                    remote_file = remote_path + file
                    try:
                        sftp.put(local_file, remote_file)
                        logger.info(file + 'file push successfully')
                    except Exception as e:
                        logger.error(e)
    except IOError:
        logger.error("remote_path or local_path is not exist")
    scp.close()


if __name__ == '__main__':
    # 读取配置文件
    config_path = './config.ini'
    scp_config = read_config(config_path)
    host_ip = str(scp_config['host_ip'])
    host_port = int(scp_config['host_port'])
    host_username = str(scp_config['host_username'])
    host_password = str(scp_config['host_password'])
    remote_path = str(scp_config['remote_path'])
    local_path = str(scp_config['local_path'])
    push_file_key = str(scp_config['push_file_key'])
    push_file_suffix = str(scp_config['push_file_suffix'])
    # 调用方法上传文件
    push(host_ip, host_port, host_username, host_password, remote_path, local_path, push_file_key, push_file_suffix)

