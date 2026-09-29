# Etapa 1: Builder
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copiar el codigo fuente completo
COPY . .

# go mod tidy analiza main.go, descarga librerias y genera go.sum
RUN go mod tidy && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o audit-worker .

# Etapa 2: Imagen Final Ultraligera
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/audit-worker .

USER nobody:nobody

CMD ["./audit-worker"]
