FROM golang:1.23-alpine AS builder

# Install git for Go modules
RUN apk add --no-cache git

WORKDIR /app

# Copy the entire project structure first
COPY . .

# Build the matching engine service
WORKDIR /app/services/matchingengine
RUN CGO_ENABLED=0 GOOS=linux go build -o matchingengine .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary
COPY --from=builder /app/services/matchingengine/matchingengine .

# Run the binary
CMD ["./matchingengine"]
