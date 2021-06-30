FROM python:3.6.13-buster

RUN mkdir /dgl_workspace
WORKDIR /dgl_workspace

RUN apt-get update
RUN apt-get install -y vim

# build the Dockerfile from the root directory
COPY examples/GraphSAGE/code code
COPY python/dglrun base
RUN mv base/tools tools
RUN mv base/requirements_dgl.txt requirements_dgl.txt
RUN chmod +x base/exec/dglrun
RUN mv base/exec/dglrun /bin/dglrun

RUN pip install --no-cache-dir -r requirements_dgl.txt