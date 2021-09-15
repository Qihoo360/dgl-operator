ARG PYPI_MIRROR=default
ARG ARCH=cpu

FROM python:3.6-slim-buster AS base

RUN mkdir /dgl_workspace
WORKDIR /dgl_workspace

RUN apt-get update
RUN apt-get install -y --no-install-recommends \
		vim \
        build-essential \
        python3-dev \
        make \
        cmake \
        git

COPY python/dglrun base
RUN mv base/tools tools
RUN mv base/requirements_dglke.txt requirements_dglke.txt
RUN chmod +x base/exec/dglkerun
RUN mv base/exec/dglkerun /bin/dglkerun

FROM base AS pypi-mirror-default
RUN pip install --no-cache-dir -r requirements_dglke.txt

FROM base AS pypi-mirror-tsinghua
RUN pip install --no-cache-dir -r requirements_dglke.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

FROM pypi-mirror-${PYPI_MIRROR} AS intermediate
RUN git clone --recursive --depth 1 --branch 0.4.3.post2 https://github.com/dmlc/dgl.git
COPY examples/DGL-KE/hotfix/tcp_socket.cc dgl/src/graph/network/tcp_socket.cc
COPY examples/DGL-KE/hotfix/dis_kvstore.py dgl/python/dgl/contrib/dis_kvstore.py

FROM intermediate AS arch-cpu
RUN cd dgl && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j4 && \
    cd ../python && \
    python setup.py install

FROM intermediate AS arch-gpu
RUN cd dgl && \
    mkdir build && \
    cd build && \
    cmake -DUSE_CUDA=ON .. && \
    make -j4 && \
    cd ../python && \
    python setup.py install

FROM arch-${ARCH} AS final
# expected commit id 7b2975a
RUN git clone --recursive --depth 1 https://github.com/awslabs/dgl-ke.git
COPY examples/DGL-KE/hotfix/dist_train.py dgl-ke/python/dglke/dist_train.py
COPY examples/DGL-KE/hotfix/kvclient.py dgl-ke/python/dglke/kvclient.py
COPY examples/DGL-KE/hotfix/kvserver.py dgl-ke/python/dglke/kvserver.py
COPY examples/DGL-KE/hotfix/sampler.py dgl-ke/python/dglke/dataloader/sampler.py

RUN cd dgl-ke/python && \
    python setup.py install
