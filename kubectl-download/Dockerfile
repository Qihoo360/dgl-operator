FROM alpine:3.11.6

WORKDIR /kubectl_download_workspace

ENV K8S_VERSION v1.20.4
RUN apk update && apk add wget
RUN wget -q https://storage.googleapis.com/kubernetes-release/release/${K8S_VERSION}/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /bin/kubectl

ENTRYPOINT ["/bin/sh", "-c"]
CMD ["cp /bin/kubectl /opt/kube/kubectl"]