FROM python:3.6.13-buster

RUN mkdir /dgl_workspace
WORKDIR /dgl_workspace

RUN apt-get update
RUN apt-get install -y vim

RUN git clone https://github.com/ryantd/dgl-distributed-base base/

RUN mv base/tools tools
RUN mv base/requirements_dglke.txt requirements_dglke.txt
RUN chmod +x base/exec/dglkerun
RUN mv base/exec/dglkerun /bin/dglkerun

RUN pip install --no-cache-dir -r requirements_dglke.txt -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

COPY hotfix/dist_train.py /usr/local/lib/python3.6/site-packages/dglke/dist_train.py
COPY hotfix/kvclient.py /usr/local/lib/python3.6/site-packages/dglke/kvclient.py
COPY hotfix/kvserver.py /usr/local/lib/python3.6/site-packages/dglke/kvserver.py
COPY hotfix/partition.py /usr/local/lib/python3.6/site-packages/dglke/partition.py
COPY hotfix/dis_kvstore.py /usr/local/lib/python3.6/site-packages/dgl/contrib/dis_kvstore.py