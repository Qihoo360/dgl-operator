# DGL Operator

The DGL Operator makes it easy to run Deep Graph Library (DGL) graph neural network distributed or non-distributed training on Kubernetes. Please check out [here](https://www.dgl.ai/) for an introduction to DGL and [dgl distributed training](https://docs.dgl.ai/guide/distributed.html) philosophy.

## 🛠Prerequisites
- Kubernetes >= 1.16

## 🚀Installation
You can deploy the operator with default settings by running the following commands:

```
git clone https://github.com/Qihoo360/dgl-operator
cd dgl-operator
kubectl create -f deploy/v1alpha1/dgl-operator.yaml
```

You can check whether the DGL Job custom resource is installed via:
```
kubectl get crd
```

The output should include `dgljobs.qihoo.net` like the following:
```
NAME                                       AGE
...
dgljobs.qihoo.net                          1m
...
```

## 🔬Creating a DGL Job
You can create a DGL job by defining an DGLJob config file. See [GraphSAGE.yaml](https://github.com/Qihoo360/dgl-operator/blob/master/examples/v1alpha1/GraphSAGE.yaml) or [GraphSAGE_dist.yaml](https://github.com/Qihoo360/dgl-operator/blob/master/examples/v1alpha1/GraphSAGE_dist.yaml) example config file for launching a __single-node__ or __multi-node__ GraphSAGE training job. You may change the config file based on your requirements.

```bash
# standalone GraphSAGE
cat examples/v1alpha1/GraphSAGE.yaml
# or a distributed version
cat examples/v1alpha1/GraphSAGE_dist.yaml
```

Deploy the DGLJob resource to start training:
```bash
# standalone GraphSAGE
kubectl create -f examples/v1alpha1/GraphSAGE.yaml
# or a distributed version
kubectl create -f examples/v1alpha1/GraphSAGE_dist.yaml
```

## 💭 Reference
Please check out these previous works that helped inspire the creation of DGL Operator

- **[PaddleFlow/paddle-operator](https://github.com/PaddleFlow/paddle-operator)** - Elastic Deep Learning Training based on Kubernetes by Leveraging EDL and Volcano.

- **[kubeflow/mpi-operator](https://github.com/kubeflow/mpi-operator)** - Kubernetes Operator for Allreduce-style Distributed Training.
