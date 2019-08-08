FROM golang:1.12

ENV GO111MODULE=on

WORKDIR /go/src/app
COPY . .

RUN go get -d -v ./...
RUN go install -v ./...

EXPOSE 80
CMD ["hicup2017", "-port", "80", "-data", "/tmp/data"]
