package shared

import "time"

type OrderBookLevel struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
	Orders   int     `json:"orders"`
}

type OrderBookSnapshot struct {
	Symbol    string           `json:"symbol"`
	Timestamp time.Time        `json:"timestamp"`
	Bids      []OrderBookLevel `json:"bids"`
	Asks      []OrderBookLevel `json:"asks"`
}

type MarketTrades struct {
	ID        string    `json:"id"`
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Quantity  float64   `json:"quantity"`
	Side      string    `json:"side"`
	Timestamp time.Time `json:"timestamp"`
}

type Ticker struct {
	Symbol             string    `json:"symbol"`
	LastPrice          float64   `json:"last_price"`
	PriceChange        float32   `json:"price_change"`
	PriceChangePercent float32   `json:"price_change_percent"`
	High24h            float64   `json:"high_24h"`
	Low24h             float64   `json:"low_24h"`
	Volume24h          float64   `json:"volume_24h"`
	QuoteVolume24h     float64   `json:"quote_volume_24h"`
	OpenPrice24h       float64   `json:"open_price_24h"`
	Timestamp          time.Time `json:"timestamp"`
}

type WSMarketMessage struct {
	Type      string      `json:"type"`
	Channel   string      `json:"channel"`
	Symbol    string      `json:"symbol"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

type WSSubscription struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	Symbol  string `json:"symbol"`
}

type WSSubscriptionResponse struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	Symbol  string `json:"symbol"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type ErrorResponse struct {
	Error     string    `json:"error"`
	Details   string    `json:"details"`
	Timestamp time.Time `json:"timestamp"`
}

// Contants
const (
	// Kafka Topics
	MARKET_DATA_TOPIC    = "market_data"
	ORDERBOOK_TOPIC      = "orderbook_updates"
	TICKER_UPDATES_TOPIC = "ticker_updates"
)

const (
	// Redis Key Patterns
	ORDERBOOK_BUY_KEY  = "orderbook:%s:buy"  // orderbook:BTCUSD:buy
	ORDERBOOK_SELL_KEY = "orderbook:%s:sell" // orderbook:BTCUSD:sell
	TRADES_KEY         = "trades:%s"         // trades:BTCUSD
	TICKER_KEY         = "ticker:%s"         // ticker:BTCUSD
	SYMBOLS_KEY        = "symbols"           // Set of all active symbols
)

const (
	// WebSocket Channels
	WS_CHANNEL_ORDERBOOK = "orderbook"
	WS_CHANNEL_TRADES    = "trades"
	WS_CHANNEL_TICKER    = "ticker"
)

const (
	// WebSocket Message Types
	WS_TYPE_SUBSCRIBE        = "subscribe"
	WS_TYPE_UNSUBSCRIBE      = "unsubscribe"
	WS_TYPE_SUBSCRIPTION_ACK = "subscription_ack"
	WS_TYPE_ORDERBOOK_UPDATE = "orderbook_update"
	WS_TYPE_TRADE            = "trade"
	WS_TYPE_TICKER_UPDATE    = "ticker_update"
)
