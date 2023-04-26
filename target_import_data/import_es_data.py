# -*- coding: utf-8 -*-

import os
from elasticsearch import Elasticsearch, helpers

from log import logger
from utils import find_file_by_suffix


# 读取json文件并批量写入ES（不存在则插入，存在则更新）
def read_json_batch_import_es(es_connect, json_path, index_name, es_timeout, es_step):
    with open(json_path, 'r', encoding='utf-8') as file:
        json_str = file.read()
        # 将字符串形式的列表数据转成列表数据
        json_list = eval(json_str)
        # 按照配置文件中的步长进行写入，缓解批量写入的压力
        length = len(json_list)
        for i in range(0, length, es_step):
            # 要写入的数据长度大于步长，那么就分批写入
            if i + es_step < length:
                actions = []
                for j in range(i, i + es_step):
                    # 先把导入时添加的"_id"的值取出来
                    new_id = json_list[j]['_id']
                    del json_list[j]["_id"]  # 删除导入时添加的"_id"
                    action = {
                        "_index": str(index_name),
                        "_id": str(new_id),
                        "_source": json_list[j]
                    }
                    actions.append(action)
                helpers.bulk(es_connect, actions, request_timeout=es_timeout)
            # 要写入的数据小于步长，那么就一次性写入
            else:
                actions = []
                for j in range(i, length):
                    # 先把导入时添加的"_id"的值取出来
                    new_id = json_list[j]['_id']
                    del json_list[j]["_id"]  # 删除导入时添加的"_id"
                    action = {
                        "_index": str(index_name),
                        "_id": str(new_id),
                        "_source": json_list[j]
                    }
                    actions.append(action)
                helpers.bulk(es_connect, actions, request_timeout=es_timeout)
        logger.info("{}数据文件：{}索引插入了{}条数据".format(json_path, str(index_name), str(length)))


# 将json数据文件导入到ES--调用入口
def import_es_data_main(target_import_dict, original_data_path):
    es_timeout = int(target_import_dict['es_timeout'])
    es_step = int(target_import_dict['es_step'])
    es_connect = Elasticsearch(
        hosts=["{}:{}".format(str(target_import_dict['es_host']),str(target_import_dict['es_port']))],
        http_auth=(str(target_import_dict['es_user']), str(target_import_dict['es_password'])),
        request_timeout=es_timeout
    )
    json_path_list = find_file_by_suffix(original_data_path, "*.json")
    for json_path in json_path_list:
        file_dir, file_full_name = os.path.split(json_path)
        index_name, file_ext = os.path.splitext(file_full_name)
        read_json_batch_import_es(es_connect, json_path, index_name, es_timeout, es_step)
        os.remove(json_path)  # 数据导入完成后删除json数据文件


