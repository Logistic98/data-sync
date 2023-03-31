# -*- coding: utf-8 -*-

import json
import os
import time
import zipfile

from Crypto.Cipher import PKCS1_OAEP, AES
from Crypto.PublicKey import RSA


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
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


# 读取指定目录下的所有文件夹保存成列表
def read_dir_to_list(file_dir_path):
    file_dir_list = os.listdir(file_dir_path)
    return file_dir_list


# 解压zip文件
def unzip_data_dir(zip_path, unzip_path):
    # 以读的方式打开
    with zipfile.ZipFile(zip_path, 'r') as f:
        for file in f.namelist():
            f.extract(file, path=unzip_path)
    os.remove(zip_path)  # 解压完后删除zip文件


# 将数据文件解密
def decrypt_file(original_data_path, encrypt_data_path, private_rsa_key_path, rsa_key):
    # 文件夹所有文件解密
    for data_file in os.listdir(encrypt_data_path):
        original_data_file_path = original_data_path + "/" + data_file
        encrypt_data_file_path = encrypt_data_path + "/" + data_file
        with open(encrypt_data_file_path, 'rb') as f_in:
            # 导入私钥
            private_key = RSA.import_key(open(private_rsa_key_path).read(), passphrase=rsa_key)
            # 会话密钥, 随机数, 消息认证码, 机密的数据
            enc_session_key, nonce, tag, cipher_text = [f_in.read(x) for x in (private_key.size_in_bytes(), 16, 16, -1)]
            cipher_rsa = PKCS1_OAEP.new(private_key)
            session_key = cipher_rsa.decrypt(enc_session_key)
            cipher_aes = AES.new(session_key, AES.MODE_EAX, nonce)
            # 解密
            data = cipher_aes.decrypt_and_verify(cipher_text, tag)
        # 文件重命名
        original_data_file_path = original_data_file_path.replace('.rsa', '')
        with open(original_data_file_path, 'wb') as f_out:
            f_out.write(data)
        os.remove(encrypt_data_file_path)  # 解密完之后删除加密文件