# -*- coding: utf-8 -*-

import glob
import os
import pymysql
import datetime
import json

from log import logger


# 重写json函数，将datetime.datetime类型的数据转化
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)


# 读取txt文件并批量写入MySQL（不存在则插入，存在则更新）
def read_txt_batch_import_mysql(mysql_connect, txt_path, table_name):
    with open(txt_path, 'r', encoding='utf-8') as file:
        txt_str = file.read()

        # 将字符串形式的列表数据转成列表数据
        txt_list = eval(txt_str)

        # 构建查询列名
        if len(txt_list) == 0:
            return None
        else:
            columns = ', '.join(txt_list[0].keys())
            qmarks = ', '.join(['%s'] * len(txt_list[0].keys()))

        # 构建查询数据
        values_list = []
        for i in txt_list:
            i = json.loads(json.dumps(i, cls=DateEncoder))
            values_item_list = []
            for j in i.values():
                values_item_list.append(j)
            values_list.append(values_item_list)

        # 批量插入数据
        cursor = mysql_connect.cursor()
        try:
            # 使用replace代替insert，实现"存在则更新，不存在则插入"的需求
            qry = "replace into " + table_name + " (%s) values (%s);" % (columns, qmarks)
            cursor.executemany(qry, values_list)
            mysql_connect.commit()
        except Exception as e:
            logger.error(e)
        cursor.close()

        logger.info("{}表插入了{}条数据".format(str(table_name), str(len(values_list))))


# 将txt数据文件导入到MySQL--调用入口
def import_mysql_data_main(target_import_dict, original_data_path):
    mysql_connect = pymysql.connect(host=str(target_import_dict['mysql_host']), user=str(target_import_dict['mysql_user']),
                            password=str(target_import_dict['mysql_password']), port=int(target_import_dict['mysql_port']),
                            db=str(target_import_dict['mysql_db']), charset='utf8')
    txt_path_list = glob.glob('{}/*.txt'.format(original_data_path))
    for txt_path in txt_path_list:
        file_dir, file_full_name = os.path.split(txt_path)
        table_name, file_ext = os.path.splitext(file_full_name)
        read_txt_batch_import_mysql(mysql_connect, txt_path, table_name)
        os.remove(txt_path)  # 数据导入完成后删除txt数据文件
    mysql_connect.close()

