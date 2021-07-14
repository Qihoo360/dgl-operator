FROM dgloperator/dgl:0.6.1-torch1.5.1-py3.6

RUN mkdir /dgl_workspace
WORKDIR /dgl_workspace

# build the Dockerfile from the root directory
COPY examples/node_classification/code code
COPY python/dglrun base

RUN mv base/tools tools
RUN chmod +x base/exec/dglrun
RUN mv base/exec/dglrun /bin/dglrun