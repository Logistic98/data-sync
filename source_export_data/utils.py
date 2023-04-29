# -*- coding: utf-8 -*-

import json
import os
import time
import zipfile

from Crypto.Cipher import PKCS1_OAEP, AES
from Crypto.PublicKey import RSA
from Crypto.Random import get_random_bytes


# 将字典写入json
def dict_to_json_file(dict, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(dict, f)


# 读取json为字典
def read_json_to_dict(path):
    with open(path, "r", encoding="utf-8") as f:
        confstr = f.read()
        conf = json.loads(confstr)
    return conf


# 获取当前时间
def get_now_time():
    now = time.localtime()
    return time.strftime("%Y%m%d%H%M%S", now), time.strftime("%Y-%m-%d %H:%M:%S", now)


# 将数据文件加密
def encrypt_data_file(original_data_path, encrypt_data_path, public_rsa_key_path):
    # 文件夹所有文件加密
    for data_file in os.listdir(original_data_path):
        original_data_file_path = original_data_path + "/" + data_file
        encrypt_data_file_path = encrypt_data_path + "/" + data_file
        # 二进制只读打开文件，读取文件数据
        with open(original_data_file_path, 'rb') as f:
            data = f.read()
        encrypt_data_file = encrypt_data_file_path + '.rsa'
        with open(encrypt_data_file, 'wb') as out_file:
            # 读取公钥
            recipient_key = RSA.import_key(open(public_rsa_key_path).read())
            # 一个 16 字节的会话密钥
            session_key = get_random_bytes(16)
            # 使用 RSA 公钥加密 Session 密钥
            cipher_rsa = PKCS1_OAEP.new(recipient_key)
            out_file.write(cipher_rsa.encrypt(session_key))
            # 使用 AES Session 密钥加密数据
            cipher_aes = AES.new(session_key, AES.MODE_EAX)
            cipher_text, tag = cipher_aes.encrypt_and_digest(data)
            out_file.write(cipher_aes.nonce)
            out_file.write(tag)
            out_file.write(cipher_text)
            # os.remove(original_data_file_path)  # 加密完之后删除原始未加密的数据文件


#  压缩加密文件夹中的所有数据文件，以开始时间-结束时间命名
def zip_data_dir(dir_path, zip_path):
    zip = zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED)
    for root, dirnames, filenames in os.walk(dir_path):
        file_path = root.replace(dir_path, '')  # 去掉根路径，只对目标文件夹下的文件及文件夹进行压缩
        for filename in filenames:
            zip.write(os.path.join(root, filename), os.path.join(file_path, filename))
            # os.remove(dir_path + '/' + filename)  # 压缩到zip文件之后删除该文件
    zip.close()
