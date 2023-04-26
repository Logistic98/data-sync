## data-sync

## 1. 项目简介

该项目是一套以离线文件形式进行数据同步的Python脚本（目前只支持MySQL、ES），支持基于时间的全量数据导出与增量数据导出。密钥采用RSA非对称加密，数据采用AES对称加密。数据文件加密后，再将其压缩成一个ZIP压缩包进行传输。适用于不可联网的重要数据进行离线数据同步。

技术说明详见我的这篇博客：[常用服务的数据备份及同步专题](https://www.eula.club/blogs/常用服务的数据备份及同步专题.html)

## 2. 项目结构

项目主要包含file_scp（远程文件传输）、rsa_key_tool（生成RSA公私钥）、source_export_data（导出数据）、target_import_data（导入数据）

```
.
├── file_scp                         
│   ├── config.ini.example
│   ├── pull.py
│   └── push.py
├── rsa_key_tool                     
│   ├── config.ini.example
│   └── rsa_encryption.py
├── source_export_data               
│   ├── clear_data_tool.py
│   ├── config.ini.example
│   ├── last_job_time.json
│   ├── export_es_data.py
│   ├── export_mysql_data.py
│   ├── gol.py
│   ├── log.py
│   ├── source_export_data.py
│   └── utils.py
└── target_import_data               
    ├── clear_data_tool.py
    ├── config.ini.example
    ├── last_job_time.json
    ├── gol.py
    ├── import_es_data.py
    ├── import_mysql_data.py
    ├── log.py
    ├── target_import_data.py
    └── utils.py
```

## 3. 项目使用

Step1：项目里的4个config.ini.example是配置示例，将其重命名为 config.ini，将配置信息换成自己的实际配置（具体含义见配置文件里的注释）

Step2：安装项目依赖，见 requirements.txt 文件（执行`pip install -r requirements.txt `命令即可）

```
schedule==1.1.0
paramiko==2.11.0
elasticsearch==7.16.2
PyMySQL==1.0.2
pycryptodome==3.17
```

Step3：执行 `rsa_key_tool/rsa_encryption.py` 生成RSA公私钥

默认在 source_export_data 目录下生成 public_rsa_key.bin 公钥文件，用于加密；在 target_import_data 目录下生成 private_rsa_key.bin 私钥文件，用于解密。

Step4：导出及导入数据

执行 `source_export_data/source_export_data.py`导出数据，通过 `file_scp/push.py`将导出的 `data_package` 同步到 `target_import_data` 目录下，执行`target_import_data/target_import_data.py`导入数据。

## 4. 注意事项

[1] 注意Python环境的版本，支持Python3.8环境，不支持Python3.9环境（加密模块会出错），其余的版本没有测试。

[2] ES的依赖版本尽量与服务端相近，我这里使用的是7.16.2版本的elasticsearch依赖，在7.16.2和8.4.1版本的服务端ES上测试无问题。

[3] 支持基于时间的全量导出与增量导出，每次执行时会读取上次的同步时间，若这个值为"init"，则跑全量数据（这里的全量是指在当前时间之前的所有数据，若时间标志位有在当前时间之后的，则不会导出）。使用的基于时间的增量字段，在配置文件的 time_field 项进行配置。需要注意的是，日期字段格式只支持`%Y-%m-%d %H:%M:%S`，若不是此格式，需要在代码里转换一下。

[4] 本项目采用 schedule 库配置定时任务，在 `source_export_data/source_export_data.py`、`target_import_data/target_import_data.py`文件里配置即可，支持配置多个定时任务规则。代码里加了运行状态限制，若上一次没执行完，本次会跳过。

```python
# 配置定时任务，可同时启动多个
logger.info("定时任务规则：{}".format("每隔30分钟运行一次job"))
schedule.every(30).minutes.do(source_export_data_main_job)
logger.info("定时任务规则：{}".format("每隔1小时运行一次job"))
schedule.every().hour.do(source_export_data_main_job)
logger.info("定时任务规则：{}".format("每天在23:59时间点运行job"))
schedule.every().day.at("23:59").do(source_export_data_main_job)
logger.info("定时任务规则：{}".format("每周一运行一次job"))
schedule.every().monday.do(source_export_data_main_job
```