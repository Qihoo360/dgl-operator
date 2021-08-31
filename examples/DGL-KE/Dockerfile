ARG PYPI_MIRROR=default

FROM python:3.6.13-buster AS base

RUN mkdir /dgl_workspace
WORKDIR /dgl_workspace

RUN apt-get update
RUN apt-get install -y vim

COPY python/dglrun base
RUN mv base/tools tools
RUN mv base/requirements_dglke.txt requirements_dglke.txt
RUN chmod +x base/exec/dglkerun
RUN mv base/exec/dglkerun /bin/dglkerun

FROM base AS mirror-default
RUN pip install --no-cache-dir -r requirements_dglke.txt

FROM base AS mirror-tsinghua
RUN pip install --no-cache-dir -r requirements_dglke.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

FROM mirror-${PYPI_MIRROR} AS final
RUN echo "PYPI_MIRROR: ${PYPI_MIRROR}"

COPY examples/DGL-KE/hotfix/dist_train.py /usr/local/lib/python3.6/site-packages/dglke/dist_train.py
COPY examples/DGL-KE/hotfix/kvclient.py /usr/local/lib/python3.6/site-packages/dglke/kvclient.py
COPY examples/DGL-KE/hotfix/kvserver.py /usr/local/lib/python3.6/site-packages/dglke/kvserver.py
COPY examples/DGL-KE/hotfix/partition.py /usr/local/lib/python3.6/site-packages/dglke/partition.py
COPY examples/DGL-KE/hotfix/dis_kvstore.py /usr/local/lib/python3.6/site-packages/dgl/contrib/dis_kvstore.py