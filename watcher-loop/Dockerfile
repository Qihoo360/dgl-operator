FROM golang:1.16 as build

WORKDIR /watcher_loop_workspace

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY main.go main.go
COPY app/ app/
COPY controllers/ controllers/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o /bin/watcher-loop main.go

FROM alpine:3.11.6
COPY --from=build /bin/watcher-loop /bin/watcher-loop
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["/bin/watcher-loop -alsologtostderr"]