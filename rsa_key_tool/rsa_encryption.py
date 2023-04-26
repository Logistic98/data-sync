# -*- coding: utf-8 -*-

from configparser import ConfigParser
from Crypto.PublicKey import RSA


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


def create_rsa_keys(code, private_rsa_key_path, public_rsa_key_path):
    """
    生成RSA私钥和公钥
    :param code: 密钥
    :return:
    """
    # 生成 2048 位的 RSA 密钥
    key = RSA.generate(2048)
    encrypted_key = key.exportKey(passphrase=code, pkcs=8, protection="scryptAndAES128-CBC")
    # 生成私钥
    with open(private_rsa_key_path, 'wb') as f:
        f.write(encrypted_key)
    # 生成公钥
    with open(public_rsa_key_path, 'wb') as f:
        f.write(key.publickey().exportKey())


if __name__ == '__main__':
    # 读取配置文件
    rsa_dict = read_config('./config.ini')
    # 在指定路径生成RSA私钥和公钥
    create_rsa_keys(rsa_dict['rsa_key'], rsa_dict['private_rsa_key_path'], rsa_dict['public_rsa_key_path'])