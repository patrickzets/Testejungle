# Multi-stage Dockerfile for wagering service
FROM golang:1.27-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates tzdata git

COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /app/wagering ./cmd/wagering

# Minimal non-root runtime
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata curl && \
    addgroup -S appgroup && \
    adduser -S -G appgroup -u 10001 appuser

WORKDIR /app

COPY --from=builder /app/wagering /app/wagering

USER appuser:appgroup

EXPOSE 8000

ENTRYPOINT ["/app/wagering"]
