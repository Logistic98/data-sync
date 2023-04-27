#!/bin/bash

## 获取当前路径、拼接挂载目录
base_path=$(cd `dirname $0`; pwd)
# 配置文件
config_path="${base_path}/config.ini"
# 日志文件
log_path="${base_path}/source_export_data.log"
# 上次同步时间的记录文件
last_job_time_path="${base_path}/last_job_time.json"
# 原始数据文件目录
original_data_base_path="${base_path}/original_data"
# 加密数据文件目录
encrypt_data_base_path="${base_path}/encrypt_data"
# 加密压缩的数据包目录
data_package_base_path="${base_path}/data_package"
# 不存在自动创建
if [ ! -d ${original_data_base_path} ];then
    mkdir ${original_data_base_path}
fi
if [ ! -d ${encrypt_data_base_path} ];then
    mkdir ${encrypt_data_base_path}
fi
if [ ! -d ${data_package_base_path} ];then
    mkdir ${data_package_base_path}
fi
if [ ! -f ${log_path} ];then
    touch ${log_path}
fi

## 构建镜像并启动容器
docker build -t source_export_data_image .
docker run -d --name source_export_data \
           -v ${config_path}:/code/config.ini \
           -v ${log_path}:/code/source_export_data.log \
           -v ${last_job_time_path}:/code/last_job_time.json \
           -v ${original_data_base_path}:/code/original_data \
           -v ${encrypt_data_base_path}:/code/encrypt_data \
           -v ${data_package_base_path}:/code/data_package \
           source_export_data_image:latest
docker update source_export_data --restart=always