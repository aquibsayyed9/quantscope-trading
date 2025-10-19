package main

import (
	"bff/notifications"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go-exchange/shared"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	log.Println("Starting BFF API Gateway...")

	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")

	bff := NewBFF(kafkaBroker)
	defer bff.Close()

	r := gin.Default()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := bff.tradeConsumer.Start(ctx); err != nil {
			log.Printf("Trade consumer error: %v", err)
		}
	}()

	// Simplified CORS middleware
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":    "ok",
			"service":   "bff",
			"timestamp": time.Now(),
		})
	})

	// Order endpoints
	r.POST("/orders", bff.PlaceOrder)
	r.DELETE("/orders/:id", bff.CancelOrder)
	r.GET("/orders/:orderID", bff.GetOrderStatus)
	r.GET("/orders", bff.GetUserOrders)

	// Test endpoint
	r.GET("/test-kafka", bff.TestKafka)

	// Notification endpoints
	r.GET("/ws/notifications", bff.HandleWebSocketConnection)
	r.GET("/notifications/status", bff.GetNotificationStatus)

	// Market data endpoints
	r.GET("/orderbook/:symbol", bff.GetOrderBook)
	r.GET("/trades/:symbol", bff.GetRecentTrades)
	r.GET("/ticker/:symbol", bff.GetTicker)
	r.GET("/ticker", bff.GetAllTickers)

	log.Println("BFF started on port 8080")

	server := &http.Server{
		Addr:           ":8080",
		Handler:        r,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 16,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatal("Server failed to start: ", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

type BFF struct {
	kafkaWriter         *kafka.Writer
	notificationManager *notifications.NotificationManager
	tradeConsumer       *notifications.TradeConsumer
	redisClient         *redis.Client

	// reduced allocations
	messagePool sync.Pool
	orderPool   sync.Pool
}

func NewBFF(kafkaBroker string) *BFF {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Topic:                  shared.ORDER_TOPIC,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireNone, // fire and forget
		Async:                  true,
		AllowAutoTopicCreation: true,
		BatchSize:              100,
		BatchTimeout:           100 * time.Microsecond,
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:            getEnv("REDIS_ADDR", "redis:6379"),
		PoolSize:        100,
		MinIdleConns:    20,
		MaxIdleConns:    50,
		ConnMaxLifetime: 30 * time.Minute,
		PoolTimeout:     1 * time.Second,
		ReadTimeout:     500 * time.Millisecond,
		WriteTimeout:    500 * time.Millisecond,
	})

	nm := notifications.NewNotificationManager()
	tc := notifications.NewTradeConsumer(kafkaBroker, nm)

	bff := &BFF{
		kafkaWriter:         writer,
		notificationManager: nm,
		tradeConsumer:       tc,
		redisClient:         rdb,
	}

	bff.initObjectPools()

	return bff
}

func (bff *BFF) initObjectPools() {
	bff.messagePool = sync.Pool{
		New: func() any {
			return &kafka.Message{}
		},
	}

	bff.orderPool = sync.Pool{
		New: func() any {
			return &shared.Order{}
		},
	}
}

func (bff *BFF) Close() {
	if bff.kafkaWriter != nil {
		bff.kafkaWriter.Close()
	}
}

func (bff *BFF) validateOrderRequest(req *PlaceOrderRequest) error {
	if req.Symbol == "" || req.Side == "" || req.Quantity <= 0 ||
		req.OrderType == "" || req.TimeInForce == "" || req.UserID == "" {
		return fmt.Errorf("invalid request: missing required fields")
	}
	return nil
}

func (bff *BFF) PlaceOrder(c *gin.Context) {
	var req PlaceOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request"})
		return
	}

	// single validation call
	if err := bff.validateOrderRequest(&req); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	// Get order from pool
	order := bff.orderPool.Get().(*shared.Order)
	*order = shared.Order{} // Reset

	// Set order fields
	order.ID = uuid.New().String()
	order.Symbol = req.Symbol
	order.Side = shared.Side(req.Side)
	order.Price = req.Price
	order.OrderType = shared.OrderType(req.OrderType)
	order.TimeInForce = shared.TimeInForce(req.TimeInForce)
	order.OriginalQty = req.Quantity
	order.RemainingQty = req.Quantity
	order.Timestamp = time.Now()
	order.UserID = req.UserID

	go func() {
		defer bff.orderPool.Put(order) // Return to pool

		if err := bff.publishOrderEventAsync("NEW", order); err != nil {
			log.Printf("Async Kafka publish failed: %v", err)
		}
	}()

	log.Printf("Order placed: %s, Symbol: %s, Side: %s, Qty: %f, Price: %f, User: %s",
		order.ID, order.Symbol, order.Side, order.OriginalQty, order.Price, order.UserID)

	// return immediately without waiting for Kafka
	c.JSON(201, gin.H{
		"order_id":      order.ID,
		"symbol":        order.Symbol,
		"side":          string(order.Side),
		"quantity":      order.OriginalQty,
		"price":         order.Price,
		"order_type":    string(order.OrderType),
		"time_in_force": string(order.TimeInForce),
		"status":        "submitted",
		"timestamp":     order.Timestamp,
	})
}

func (bff *BFF) CancelOrder(c *gin.Context) {
	orderID := c.Param("id")
	symbol := c.Query("symbol")

	if symbol == "" {
		c.JSON(400, gin.H{"error": "symbol query parameter is required"})
		return
	}

	go func() {
		order := &shared.Order{ID: orderID, Symbol: symbol}
		if err := bff.publishOrderEventAsync("CANCEL", order); err != nil {
			log.Printf("Failed to publish cancel order: %v", err)
		}
	}()

	log.Printf("Cancel order requested: %s for symbol %s", orderID, symbol)

	c.JSON(200, gin.H{
		"order_id":  orderID,
		"symbol":    symbol,
		"status":    "cancel_requested",
		"timestamp": time.Now(),
	})
}

func (bff *BFF) TestKafka(c *gin.Context) {
	go func() {
		testEvent := shared.OrderEvent{
			Type:      "TEST",
			Order:     nil,
			Timestamp: time.Now(),
		}

		data, _ := json.Marshal(testEvent)
		message := kafka.Message{
			Key:   []byte("test"),
			Value: data,
		}

		if err := bff.kafkaWriter.WriteMessages(context.Background(), message); err != nil {
			log.Printf("Test Kafka message failed: %v", err)
		}
	}()

	c.JSON(200, gin.H{
		"status":    "Kafka test message sent",
		"timestamp": time.Now(),
	})
}

func (bff *BFF) publishOrderEventAsync(eventType string, order *shared.Order) error {
	event := shared.OrderEvent{
		Type:      eventType,
		Order:     order,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Get message from pool
	message := bff.messagePool.Get().(*kafka.Message)
	defer bff.messagePool.Put(message)

	message.Key = []byte(order.Symbol)
	message.Value = data

	// Fire-and-forget write
	return bff.kafkaWriter.WriteMessages(context.Background(), *message)
}

func (bff *BFF) HandleWebSocketConnection(c *gin.Context) {
	bff.notificationManager.HandleWebSocket(c)
}

func (bff *BFF) GetNotificationStatus(c *gin.Context) {
	count := bff.notificationManager.GetConnectionCount()
	c.JSON(200, gin.H{
		"connected_users": count,
		"status":          "active",
	})
}

func (bff *BFF) GetOrderStatus(c *gin.Context) {
	orderID := c.Param("orderID")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	orderHistory, err := bff.getOrderHistoryFromRedis(ctx, orderID)
	if err != nil {
		c.JSON(404, gin.H{"error": "Order not found"})
		return
	}

	c.JSON(200, orderHistory)
}

func (bff *BFF) GetUserOrders(c *gin.Context) {
	userID := c.Query("user_id")
	status := c.Query("status")

	if userID == "" {
		c.JSON(400, gin.H{"error": "user_id query parameter is required"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	orders, err := bff.getUserOrdersFromRedis(ctx, userID, status)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve orders"})
		return
	}

	c.JSON(200, gin.H{
		"orders": orders,
		"count":  len(orders),
	})
}

func (bff *BFF) getOrderHistoryFromRedis(ctx context.Context, orderID string) (*shared.OrderHistory, error) {
	key := fmt.Sprintf("order_status:%s", orderID)

	data, err := bff.redisClient.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var orderHistory shared.OrderHistory
	err = json.Unmarshal([]byte(data), &orderHistory)
	return &orderHistory, err
}

func (bff *BFF) getUserOrdersFromRedis(ctx context.Context, userID, statusFilter string) ([]*shared.OrderHistory, error) {
	pattern := "order_status:*"

	keys, err := bff.redisClient.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, err
	}

	var userOrders []*shared.OrderHistory

	// Batch Redis calls
	pipe := bff.redisClient.Pipeline()
	cmds := make(map[string]*redis.StringCmd)

	for _, key := range keys {
		cmds[key] = pipe.Get(ctx, key)
	}

	pipe.Exec(ctx)

	for _, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil {
			continue
		}

		var orderHistory shared.OrderHistory
		if err := json.Unmarshal([]byte(data), &orderHistory); err != nil {
			continue
		}

		if orderHistory.UserID != userID {
			continue
		}

		if statusFilter != "" {
			if len(orderHistory.StatusHistory) == 0 {
				continue
			}
			latestStatus := orderHistory.StatusHistory[len(orderHistory.StatusHistory)-1]
			if string(latestStatus.Status) != statusFilter {
				continue
			}
		}

		userOrders = append(userOrders, &orderHistory)
	}

	return userOrders, nil
}

func (bff *BFF) getOrderBookFromRedis(symbol string, limit int) (*shared.OrderBookSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// OPTIMIZATION: Pipeline Redis calls
	pipe := bff.redisClient.Pipeline()
	buyCmd := pipe.Get(ctx, fmt.Sprintf("orderbook:%s:buy", symbol))
	sellCmd := pipe.Get(ctx, fmt.Sprintf("orderbook:%s:sell", symbol))
	pipe.Exec(ctx)

	buyData, _ := buyCmd.Result()
	sellData, _ := sellCmd.Result()

	var buyOrders, sellOrders []*shared.Order
	json.Unmarshal([]byte(buyData), &buyOrders)
	json.Unmarshal([]byte(sellData), &sellOrders)

	var bids, asks []shared.OrderBookLevel
	for _, o := range buyOrders {
		bids = append(bids, shared.OrderBookLevel{Price: o.Price, Quantity: o.RemainingQty, Orders: 1})
	}
	for _, o := range sellOrders {
		asks = append(asks, shared.OrderBookLevel{Price: o.Price, Quantity: o.RemainingQty, Orders: 1})
	}

	return &shared.OrderBookSnapshot{Symbol: symbol, Timestamp: time.Now(), Bids: bids, Asks: asks}, nil
}

func (bff *BFF) getRecentTradesFromRedis(symbol string, limit int) ([]*shared.MarketTrades, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	tradeData, _ := bff.redisClient.LRange(ctx, fmt.Sprintf("trades:%s", symbol), 0, int64(limit-1)).Result()

	var trades []*shared.MarketTrades
	for _, data := range tradeData {
		var trade shared.Trade
		json.Unmarshal([]byte(data), &trade)
		trades = append(trades, &shared.MarketTrades{
			ID: trade.ID, Symbol: trade.Symbol, Price: trade.Price,
			Quantity: trade.Qty, Side: trade.MakerSide, Timestamp: trade.Timestamp,
		})
	}
	return trades, nil
}

func (bff *BFF) calculateTickerFromRedis(symbol string) (*shared.Ticker, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	tradeData, _ := bff.redisClient.LRange(ctx, fmt.Sprintf("trades:%s", symbol), 0, -1).Result()

	var lastPrice, high24h, low24h, volume24h float64
	for _, data := range tradeData {
		var trade shared.Trade
		json.Unmarshal([]byte(data), &trade)
		lastPrice = trade.Price
		if trade.Price > high24h {
			high24h = trade.Price
		}
		if low24h == 0 || trade.Price < low24h {
			low24h = trade.Price
		}
		volume24h += trade.Qty
	}

	return &shared.Ticker{
		Symbol: symbol, LastPrice: lastPrice, High24h: high24h,
		Low24h: low24h, Volume24h: volume24h, Timestamp: time.Now(),
	}, nil
}

func (bff *BFF) GetOrderBook(c *gin.Context) {
	symbol := strings.ToUpper(c.Param("symbol"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "10"))

	orderBook, err := bff.getOrderBookFromRedis(symbol, limit)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve order book"})
		return
	}
	c.JSON(200, orderBook)
}

func (bff *BFF) GetRecentTrades(c *gin.Context) {
	symbol := strings.ToUpper(c.Param("symbol"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))

	trades, err := bff.getRecentTradesFromRedis(symbol, limit)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve trades"})
		return
	}
	c.JSON(200, trades)
}

func (bff *BFF) GetTicker(c *gin.Context) {
	symbol := strings.ToUpper(c.Param("symbol"))

	ticker, err := bff.calculateTickerFromRedis(symbol)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to calculate ticker"})
		return
	}
	c.JSON(200, ticker)
}

func (bff *BFF) GetAllTickers(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	symbols, _ := bff.redisClient.SMembers(ctx, "symbols").Result()

	tickers := []*shared.Ticker{}
	for _, symbol := range symbols {
		if ticker, err := bff.calculateTickerFromRedis(symbol); err == nil {
			tickers = append(tickers, ticker)
		}
	}
	c.JSON(200, gin.H{"tickers": tickers})
}
