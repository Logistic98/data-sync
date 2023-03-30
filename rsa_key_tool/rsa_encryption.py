# -*- coding: utf-8 -*-

from configparser import RawConfigParser
from Crypto.PublicKey import RSA


def read_config():
    cfg = RawConfigParser()
    cfg.read('./config.ini', encoding='utf-8')
    rsa_key = cfg.get('RSA', 'rsa_key')
    private_rsa_key_path = cfg.get('RSA', 'private_rsa_key_path')
    public_rsa_key_path = cfg.get('RSA', 'public_rsa_key_path')
    rsa_config = {}
    rsa_config['rsa_key'] = rsa_key
    rsa_config['private_rsa_key_path'] = private_rsa_key_path
    rsa_config['public_rsa_key_path'] = public_rsa_key_path
    return rsa_config


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
    rsa_dict = read_config()
    # 在指定路径生成RSA私钥和公钥
    create_rsa_keys(rsa_dict['rsa_key'], rsa_dict['private_rsa_key_path'], rsa_dict['public_rsa_key_path'])