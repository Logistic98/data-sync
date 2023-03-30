## data-sync

## 1. 项目简介

该项目是一套以离线文件形式进行数据同步的Python脚本（目前只支持MySQL、ES），支持基于时间的全量数据同步与增量数据同步。密钥采用RSA非对称加密，数据采用AES对称加密。数据文件加密后，再将其压缩成一个ZIP压缩包进行传输。适用于不可联网的重要数据进行离线数据同步。

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
│   ├── export_es_data.py
│   ├── export_mysql_data.py
│   ├── gol.py
│   ├── log.py
│   ├── source_export_data.py
│   └── utils.py
└── target_import_data               
    ├── clear_data_tool.py
    ├── config.ini.example
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

[1] ES的依赖版本尽量与服务端相近，我这里使用的是7.16.2版本的elasticsearch依赖，在7.16.2和8.4.1版本的服务端ES上测试无问题。

[2] 支持基于时间的全量同步与增量同步，每次执行时会读取上次的同步时间，若这个值为空，则跑全量数据。使用的时间标志位为 update_time，将其写死在代码里了，如果不一致的话，需要对其进行修改。