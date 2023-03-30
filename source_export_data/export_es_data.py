# -*- coding: utf-8 -*-

import json
from elasticsearch import Elasticsearch

from log import logger


# 将ES查询出来list写入到json文件
def write_list_to_json(list, json_file_path):
    with open(json_file_path, 'w', encoding='utf-8') as f:
        json.dump(list, f, ensure_ascii=False)


# 将符合条件的ES数据查出保存为json
def es_export_json(es_connect, es_size, es_scroll, index_list, original_data_path, last_job_time, now_time):
    for i in index_list:
        logger.info("正在保存{}索引的数据".format(i))
        if last_job_time != "":
            query = {
                "range": {
                    "update_time": {
                        # 大于上一次读取结束时间，小于等于本次读取开始时间
                        "gt": last_job_time,
                        "lte": now_time
                    }
                }
            }
            logger.info("增量导出，时间范围为{}至{}".format(last_job_time, now_time))
        else:
            query = {
                "range": {
                    "update_time": {
                        # 小于等于本次读取开始时间
                        "lte": now_time
                    }
                }
            }
            logger.info("全量导出，时间范围为{}之前".format(now_time))
        try:
            source_list = []
            # 滚动查询符合条件的所有es数据
            page = es_connect.search(index=i, query=query, size=es_size, scroll=es_scroll)
            for hit in page['hits']['hits']:
                source_data = hit['_source']
                source_data['_id'] = hit['_id']
                source_list.append(source_data)
            # 游标用于输出es查询出的所有结果
            sid = page['_scroll_id']
            # es查询出的结果总量
            scroll_size = page['hits']['total']['value']
            while (scroll_size > 0):
                page = es_connect.scroll(scroll_id=sid, scroll=es_scroll)
                sid = page['_scroll_id']
                scroll_size = len(page['hits']['hits'])
                for hit in page['hits']['hits']:
                    source_data = hit['_source']
                    source_data['_id'] = hit['_id']
                    source_list.append(source_data)
            json_file_path = original_data_path + "/" + str(i) + ".json"
            if len(source_list) != 0:
                write_list_to_json(source_list, json_file_path)
                logger.info('{}索引的数据已保存至{}路径，导出的数据总量为{}'.format(str(i), json_file_path, str(len(source_list))))
            else:
                logger.info('{}索引无更新'.format(str(i)))
        except Exception as e:
            logger.error("ES索引数据导出至JSON文件的过程出错：{}".format(e))


# 将符合条件的ES数据查出保存为json--调用入口
def export_es_data_main(source_export_dict, original_data_path, last_job_time, now_time):
    es_connect = Elasticsearch(
        hosts=[str(source_export_dict['es_host']) + ":" + str(source_export_dict['es_port'])],
        http_auth=(str(source_export_dict['es_user']), str(source_export_dict['es_password'])),
        request_timeout=int(source_export_dict['es_timeout'])
    )
    index_list = ''.join(source_export_dict['es_index_list'].split()).split(",")
    es_size = int(source_export_dict['es_size'])
    es_scroll = str(source_export_dict['es_scroll'])
    es_export_json(es_connect, es_size, es_scroll, index_list, original_data_path, last_job_time, now_time)