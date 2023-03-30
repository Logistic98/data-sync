# -*- coding: utf-8 -*-

import pymysql
from log import logger


# 将符合条件的MySQL数据查出保存为txt
def mysql_export_txt(mysql_connect, table_list, original_data_path, last_job_time, now_time):
    for table_name in table_list:
        logger.info("正在保存{}表的数据".format(table_name))
        data_list = []
        if last_job_time == "":
            sql = "SELECT * FROM {} where update_time <= '{}'".format(table_name, str(now_time))
        else:
            sql = "SELECT * FROM {} where update_time <= '{}' and update_time > '{}'".format(table_name, str(now_time), str(last_job_time))
        logger.info("执行的sql语句为{}".format(sql))
        try:
            cursor = mysql_connect.cursor()
            cursor.execute(sql)
            desc = cursor.description  # 获取字段的描述
            result = [dict(zip([col[0] for col in desc], row)) for row in cursor.fetchall()]   # 以字典的形式返回数据
            for row in result:
                data_list.append(row)
        except Exception as e:
            logger.error("MySQL表数据导出至txt文件的过程出错：{}".format(e))
        txt_path = original_data_path + "/" + table_name + ".txt"
        with open(txt_path, "w+", encoding='utf-8') as f:
            f.write(str(data_list))
            f.close()
        logger.info('{}表的数据已保存至{}路径，导出的数据总量为{}'.format(table_name, txt_path, str(len(data_list))))


# 将符合条件的MySQL数据查出保存为txt--调用入口
def export_mysql_data_main(source_export_dict, original_data_path, last_job_time, now_time):
    mysql_connect = pymysql.connect(host=str(source_export_dict['mysql_host']), user=str(source_export_dict['mysql_user']),
                            password=str(source_export_dict['mysql_password']), port=int(source_export_dict['mysql_port']),
                            db=str(source_export_dict['mysql_db']), charset='utf8')
    table_list = ''.join(source_export_dict['mysql_table_list'].split()).split(",")
    mysql_export_txt(mysql_connect, table_list, original_data_path, last_job_time, now_time)
    mysql_connect.close()