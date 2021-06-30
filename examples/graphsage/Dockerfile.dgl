FROM python:3.6.13-buster

RUN mkdir /dgl
WORKDIR /dgl

RUN apt-get update
RUN apt-get install -y vim

# build the Dockerfile from the root directory
COPY python/dglrun/requirements_dgl.txt requirements_dgl.txt

RUN pip install --no-cache-dir -r requirements_dgl.txt