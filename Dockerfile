# Stage 1: Build the Go binary
FROM golang:1.26-alpine AS builder

# Install git and other necessary dependencies
RUN apk add --no-cache git

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum (if exists) and download dependencies
COPY go.mod ./
# COPY go.sum ./
RUN go mod download

# Copy the entire source code into the container
COPY . .

# Build the bitcask server binary statically
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bitcask-server ./cmd/bitcask-server

# Stage 2: Create a minimal production image
FROM alpine:latest

# Install tzdata and ca-certificates just in case
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/bitcask-server .

# Set default environment variables
ENV PORT=9092
ENV BITCASK_DIR=/bitcask/data

# Expose the default server port
EXPOSE $PORT

# Create the data directory
RUN mkdir -p /bitcask/data

# Set the entrypoint to run the bitcask server
ENTRYPOINT ["./bitcask-server"]
