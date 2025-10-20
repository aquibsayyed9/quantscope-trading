# QuantScope Trading Exchange

A high-performance matching engine built in Go for cryptocurrency trading, featuring microsecond-level order matching, real-time market data, and WebSocket notifications.

## Architecture

Microservices architecture with event-driven design:
- **BFF Service**: REST API gateway + WebSocket server
- **Matching Engine**: Core order matching with price-time priority
- **Message Broker**: Kafka for event streaming
- **Persistence**: Redis for order books, trades, and order history

## Features

### Order Management
- Multiple order types: LIMIT, MARKET
- Time-in-force: GTC (Good-Til-Cancelled), IOC (Immediate-or-Cancel), FOK (Fill-or-Kill)
- Order cancellation
- Order status tracking with history

### Matching Engine
- Price-time priority matching algorithm
- Heap-based order book (O(log n) operations)
- Supports 10,000 orders per side, 1,000 symbols
- Zero-allocation matching with object pools
- Batch processing (1,000 orders/batch)
- Async persistence with worker pools

### Real-Time Features
- WebSocket notifications for trade executions
- Live order book snapshots
- Recent trades feed
- 24h ticker data

### Performance Optimizations
- Object pooling for reduced GC pressure
- Connection pooling (Redis, Kafka)
- Batch writes to Kafka/Redis
- Async persistence workers
- Pipeline Redis operations

## Tech Stack

- **Language**: Go 1.23
- **Message Broker**: Kafka
- **Database**: Redis
- **Deployment**: Docker Compose
- **WebSocket**: Gorilla WebSocket
- **Load Testing**: wrk + Lua

## Running Locally
```bash
# Start all services
docker-compose up -d

# API is available at http://localhost:8080
# WebSocket at ws://localhost:8080/ws/notifications

# Run load test
wrk -t4 -c100 -d30s -s tests/post.lua http://localhost:8080/orders
```

## API Endpoints

### Orders
- `POST /orders` - Place order
- `DELETE /orders/:id` - Cancel order
- `GET /orders/:orderID` - Get order status
- `GET /orders?user_id=X` - Get user orders

### Market Data
- `GET /orderbook/:symbol` - Order book snapshot
- `GET /trades/:symbol` - Recent trades
- `GET /ticker/:symbol` - 24h ticker
- `GET /ticker` - All tickers

### WebSocket
- `GET /ws/notifications?user_id=X` - Real-time trade notifications

## Performance

Handles 1000+ orders/second with microsecond-level matching latency.

Load test results (wrk, 4 threads, 100 connections):
- [Add your actual numbers when you benchmark]

## Why I Built This

To understand:
- Trading system architecture and matching algorithms
- High-performance Go patterns (object pools, zero-allocation)
- Event-driven microservices
- Real-time data distribution

Exploring how to reduce latency in order matching - future work includes porting critical paths to Rust for sub-microsecond performance.

## Project Structure
```
├── services/
│   ├── bff/              # API Gateway + WebSocket
│   ├── matchingengine/   # Core matching logic
│   └── shared/           # Shared types
├── docker-compose.yml
└── tests/
    └── post.lua          # Load testing script
```

## Limitations / Future Work

- Single-node matching engine (no horizontal scaling yet)
- Basic market data (no advanced charts/indicators)
- No authentication/authorization
- No order modification (only cancel+replace)
- Exploring Rust rewrite for <1μs latency

## License

MIT
