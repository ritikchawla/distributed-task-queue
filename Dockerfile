# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git ca-certificates

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binaries
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /bin/dtq-server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /bin/dtq-worker ./cmd/worker

# Final stage - Server
FROM alpine:3.19 AS server

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/dtq-server /app/dtq-server

EXPOSE 8080

ENTRYPOINT ["/app/dtq-server"]

# Final stage - Worker
FROM alpine:3.19 AS worker

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/dtq-worker /app/dtq-worker

ENTRYPOINT ["/app/dtq-worker"]
