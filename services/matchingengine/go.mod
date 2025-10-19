module matchingengine

go 1.23.2

replace go-exchange/shared => ../../shared

require (
	github.com/google/uuid v1.5.0
	github.com/redis/go-redis/v9 v9.3.0
	github.com/segmentio/kafka-go v0.4.46
	go-exchange/shared v0.0.0-00010101000000-000000000000
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/stretchr/testify v1.8.3 // indirect
)
