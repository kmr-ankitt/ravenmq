FROM golang:1.27-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/coordinator ./cmd/coordinator && \
    CGO_ENABLED=0 GOOS=linux go build -o /out/broker ./cmd/broker

FROM alpine:3.21
COPY --from=builder /out/coordinator /usr/local/bin/coordinator
COPY --from=builder /out/broker /usr/local/bin/broker