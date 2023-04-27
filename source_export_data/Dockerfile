# 基于python3.7镜像创建新镜像
FROM python:3.7
# 创建容器内部目录
RUN mkdir /code
# 将项目复制到内部目录
ADD . /code/
# 切换到工作目录
WORKDIR /code
# 安装项目依赖
RUN pip install schedule==1.1.0
RUN pip install paramiko==2.11.0
RUN pip install elasticsearch==7.16.2
RUN pip install PyMySQL==1.0.2
RUN pip install pycryptodome==3.17
# 启动项目
ENTRYPOINT ["nohup","python","source_export_data.py","&"]