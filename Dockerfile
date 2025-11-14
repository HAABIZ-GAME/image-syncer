FROM golang:1.25

WORKDIR /go/src/github.com/haabiz-game/image-syncer

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV APP_BIN=/go/bin/octops-image-syncer \
    VERSION=v0.1.1 \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

RUN make build

FROM gcr.io/distroless/static:nonroot

COPY --from=build-env /go/bin/octops-image-syncer /

ENTRYPOINT ["/octops-image-syncer"]
