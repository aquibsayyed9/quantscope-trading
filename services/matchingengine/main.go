package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go-exchange/shared"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const (
	MAX_ORDERS_PER_SIDE = 10000 // 10K orders per side (buy/sell)
	MAX_TOTAL_ORDERS    = 20000 // Total orders per symbol
	MAX_SYMBOLS         = 1000  // Maximum symbols supported
)

func main() {
	log.Println("Starting Matching Engine...")

	engine := NewMatchingEngine(
		getEnv("REDIS_ADDR", "redis:6379"),
		getEnv("KAFKA_BROKER", "kafka:9092"),
	)

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		log.Println("Shutting down gracefully...")
		cancel()
	}()

	engine.Start(ctx)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// ==============================================
// MATCHING ENGINE
// ==============================================

type MatchingEngine struct {
	orderBooks  *OrderBookManager
	kafkaReader *kafka.Reader
	kafkaWriter *kafka.Writer

	// Object pools for zero-allocation matching
	tradePool sync.Pool
	slicePool sync.Pool

	// Worker pools for async processing
	persistWorkers []chan PersistenceTask
	workerCount    int

	// Shutdown coordination
	shutdownCh chan struct{}
	wg         sync.WaitGroup
}

type PersistenceTask struct {
	Type   string
	Data   interface{}
	Symbol string
}

func NewMatchingEngine(redisAddr, kafkaBroker string) *MatchingEngine {
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		PoolSize:     200,
		MinIdleConns: 50,
	})

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBroker),
		Topic:                  shared.TRADE_TOPIC,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
		BatchSize:              1000,
		BatchTimeout:           10 * time.Millisecond,
		Async:                  true,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    shared.ORDER_TOPIC,
		GroupID:  "matching-engine",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  1 * time.Millisecond,

		Dialer: &kafka.Dialer{
			Timeout:   30 * time.Second,
			DualStack: true,
			KeepAlive: 30 * time.Second,
		},

		ReadBatchTimeout: 1 * time.Millisecond,
	})

	// Initialize persistence workers
	workerCount := runtime.NumCPU()
	if workerCount < 4 {
		workerCount = 4
	}

	persistWorkers := make([]chan PersistenceTask, workerCount)
	for i := 0; i < workerCount; i++ {
		persistWorkers[i] = make(chan PersistenceTask, 10000)
	}

	obm := NewOrderBookManager(rdb, writer, persistWorkers)

	engine := &MatchingEngine{
		orderBooks:     obm,
		kafkaReader:    reader,
		kafkaWriter:    writer,
		workerCount:    workerCount,
		persistWorkers: persistWorkers,
		shutdownCh:     make(chan struct{}),
	}

	engine.initObjectPools()
	engine.startPersistenceWorkers()

	return engine
}

func (me *MatchingEngine) initObjectPools() {
	me.tradePool = sync.Pool{
		New: func() interface{} {
			return &shared.Trade{}
		},
	}

	me.slicePool = sync.Pool{
		New: func() interface{} {
			return make([]*shared.Trade, 0, 50)
		},
	}
}

func (me *MatchingEngine) startPersistenceWorkers() {
	for i := 0; i < me.workerCount; i++ {
		me.wg.Add(1)
		go me.persistenceWorker(i, me.persistWorkers[i])
	}
}

func (me *MatchingEngine) persistenceWorker(workerID int, taskChan chan PersistenceTask) {
	defer me.wg.Done()

	batch := make([]PersistenceTask, 0, 1000)
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-me.shutdownCh:
			if len(batch) > 0 {
				me.flushBatch(batch)
			}
			return

		case task := <-taskChan:
			batch = append(batch, task)
			if len(batch) >= 1000 {
				me.flushBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				me.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (me *MatchingEngine) flushBatch(batch []PersistenceTask) {
	for _, task := range batch {
		switch task.Type {
		case "TRADE":
			if trade, ok := task.Data.(*shared.Trade); ok {
				me.orderBooks.storeTradeInRedis(trade)
				me.orderBooks.publishTradeEvent(trade)
			}
		case "ORDER_UPDATE":
			if order, ok := task.Data.(*shared.Order); ok {
				me.orderBooks.addOrderToRedis(order)
			}
		case "ORDER_STATUS":
			if order, ok := task.Data.(*shared.Order); ok {
				if order.OrderStatus == shared.COMPLETE {
					me.orderBooks.statusManager.UpdateOrderStatus(order, shared.COMPLETE, "Order fully executed")
				} else if order.RemainingQty < order.OriginalQty {
					me.orderBooks.statusManager.UpdateOrderStatus(order, shared.PARTIALLY_FILLED, "Order partially executed")
				}
			}
		}
	}
}

func (me *MatchingEngine) Start(ctx context.Context) {
	log.Println("Matching Engine started, waiting for orders...")

	for {
		select {
		case <-ctx.Done():
			me.kafkaReader.Close()
			me.kafkaWriter.Close()
			return
		default:
			start := time.Now()

			var messages []kafka.Message
			batchSize := 1000

			for i := 0; i < batchSize; i++ {
				msgCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
				message, err := me.kafkaReader.FetchMessage(msgCtx)
				cancel()

				if err != nil {
					break
				}
				messages = append(messages, message)
			}

			if len(messages) == 0 {
				continue
			}

			kafkaTime := time.Since(start)

			processStart := time.Now()
			for _, message := range messages {
				unmarshalStart := time.Now()
				var orderEvent shared.OrderEvent
				if err := json.Unmarshal(message.Value, &orderEvent); err != nil {
					log.Printf("Error unmarshaling order: %v", err)
					continue
				}
				unmarshalTime := time.Since(unmarshalStart)

				eventStart := time.Now()
				me.processOrderEvent(orderEvent)
				eventTime := time.Since(eventStart)

				if len(messages) == 1 {
					total := time.Since(start)
					log.Printf("Timing: Total=%v Kafka=%v JSON=%v Process=%v",
						total, kafkaTime, unmarshalTime, eventTime)
				}
			}
			processTime := time.Since(processStart)

			if err := me.kafkaReader.CommitMessages(ctx, messages...); err != nil {
				log.Printf("Error committing batch: %v", err)
			}

			if len(messages) > 1 {
				total := time.Since(start)
				avgPerMessage := total / time.Duration(len(messages))
				log.Printf("Batch: %d msgs, Total=%v, Kafka=%v, Process=%v, Avg=%v/msg",
					len(messages), total, kafkaTime, processTime, avgPerMessage)
			}
		}
	}
}

func (me *MatchingEngine) processOrderEvent(event shared.OrderEvent) {
	switch event.Type {
	case "NEW":
		trades, err := me.orderBooks.ProcessOrder(event.Order)
		if err != nil {
			log.Printf("Error occurred while process order: %s", err)
		}
		log.Printf("Processed order %s, generated %d trades", event.Order.ID, len(trades))
	case "CANCEL":
		err := me.orderBooks.CancelOrder(event.Order.ID, event.Order.Symbol)
		if err != nil {
			log.Printf("Error canceling order: %v", err)
		}
	}
}

// ==============================================
// ORDER BOOK MANAGER
// ==============================================

type OrderBookManager struct {
	books          map[string]*OrderBook
	mutex          sync.RWMutex
	redisClient    *redis.Client
	kafkaWriter    *kafka.Writer
	statusManager  *OrderStatusManager
	persistWorkers []chan PersistenceTask
	workerCount    int
	totalSymbols   int
	maxSymbols     int
}

func NewOrderBookManager(redisClient *redis.Client, kafkaWriter *kafka.Writer, persistWorkers []chan PersistenceTask) *OrderBookManager {
	return &OrderBookManager{
		books:          make(map[string]*OrderBook, MAX_SYMBOLS),
		redisClient:    redisClient,
		kafkaWriter:    kafkaWriter,
		statusManager:  NewOrderStatusManager(redisClient, kafkaWriter),
		persistWorkers: persistWorkers,
		workerCount:    len(persistWorkers),
		totalSymbols:   0,
		maxSymbols:     MAX_SYMBOLS,
	}
}

func (obm *OrderBookManager) hashOrder(order *shared.Order) uint32 {
	hash := uint32(2166136261)
	for _, b := range []byte(order.ID) {
		hash ^= uint32(b)
		hash *= 16777619
	}
	return hash
}

func (obm *OrderBookManager) GetOrderBook(symbol string) (*OrderBook, error) {
	obm.mutex.Lock()
	defer obm.mutex.Unlock()

	if book, exists := obm.books[symbol]; exists {
		return book, nil
	}

	buyHeap := &OrderHeap{orders: make([]*shared.Order, MAX_ORDERS_PER_SIDE), isBuy: true}
	sellHeap := &OrderHeap{orders: make([]*shared.Order, MAX_ORDERS_PER_SIDE), isBuy: false}
	heap.Init(buyHeap)
	heap.Init(sellHeap)

	book := &OrderBook{
		Symbol:           symbol,
		BuyOrders:        buyHeap,
		SellOrders:       sellHeap,
		MaxOrdersPerSide: MAX_ORDERS_PER_SIDE,
		TotalOrders:      0,
	}

	obm.restoreOrderBookFromRedis(book)
	obm.books[symbol] = book
	obm.totalSymbols++

	log.Printf("Created order book for %s (total symbols: %d)", symbol, obm.totalSymbols)
	return book, nil
}

func (obm *OrderBookManager) ProcessOrder(order *shared.Order) ([]*shared.Trade, error) {
	book, err := obm.GetOrderBook(order.Symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to get order book: %w", err)
	}

	book.mutex.Lock()
	trades, ordersToUpdate := book.Match(order, obm)
	book.mutex.Unlock()

	if len(trades) > 0 || order.OrderStatus == shared.PENDING {
		workerID := int(obm.hashOrder(order)) % obm.workerCount

		for _, trade := range trades {
			select {
			case obm.persistWorkers[workerID] <- PersistenceTask{"TRADE", trade, order.Symbol}:
			default:
				log.Printf("Warning: Persistence queue full for worker %d", workerID)
			}
		}

		if order.OrderStatus == shared.PENDING && order.RemainingQty > 0 {
			select {
			case obm.persistWorkers[workerID] <- PersistenceTask{"ORDER_UPDATE", order, order.Symbol}:
			default:
				log.Printf("Warning: Persistence queue full for order update")
			}
		}

		if order.OrderStatus == shared.COMPLETE || order.RemainingQty < order.OriginalQty {
			select {
			case obm.persistWorkers[workerID] <- PersistenceTask{"ORDER_STATUS", order, order.Symbol}:
			default:
				log.Printf("Warning: Persistence queue full for status update")
			}
		}

		for _, orderToUpdate := range ordersToUpdate {
			select {
			case obm.persistWorkers[workerID] <- PersistenceTask{"ORDER_STATUS", orderToUpdate, orderToUpdate.Symbol}:
			default:
				log.Printf("Warning: Persistence queue full for matched order status")
			}
		}
	}
	return trades, nil
}

func (obm *OrderBookManager) CancelOrder(orderID, symbol string) error {
	book, err := obm.GetOrderBook(symbol)
	if err != nil {
		return err
	}

	book.mutex.Lock()
	defer book.mutex.Unlock()

	for i, order := range book.BuyOrders.orders {
		if order.ID == orderID {
			order.OrderStatus = shared.CANCELLED
			book.BuyOrders.orders = append(book.BuyOrders.orders[:i], book.BuyOrders.orders[i+1:]...)
			heap.Init(book.BuyOrders)
			book.TotalOrders--
			go obm.removeOrderFromRedis(symbol, string(shared.BUY), orderID)
			return nil
		}
	}

	for i, order := range book.SellOrders.orders {
		if order.ID == orderID {
			order.OrderStatus = shared.CANCELLED
			book.SellOrders.orders = append(book.SellOrders.orders[:i], book.SellOrders.orders[i+1:]...)
			heap.Init(book.SellOrders)
			book.TotalOrders--
			go obm.removeOrderFromRedis(symbol, string(shared.SELL), orderID)
			return nil
		}
	}

	return fmt.Errorf("order not found: %s", orderID)
}

// ==============================================
// ORDER BOOK HEAP IMPLEMENTATION
// ==============================================

type OrderHeap struct {
	orders []*shared.Order
	isBuy  bool
}

func (h OrderHeap) Len() int { return len(h.orders) }

func (h OrderHeap) Less(i, j int) bool {
	if h.isBuy {
		if h.orders[i].Price != h.orders[j].Price {
			return h.orders[i].Price > h.orders[j].Price
		}
	} else {
		if h.orders[i].Price != h.orders[j].Price {
			return h.orders[i].Price < h.orders[j].Price
		}
	}
	return h.orders[i].Timestamp.Before(h.orders[j].Timestamp)
}

func (h OrderHeap) Swap(i, j int) {
	h.orders[i], h.orders[j] = h.orders[j], h.orders[i]
}

func (h *OrderHeap) Push(x any) {
	h.orders = append(h.orders, x.(*shared.Order))
}

func (h *OrderHeap) Pop() any {
	old := h.orders
	n := len(old)
	item := old[n-1]
	h.orders = old[0 : n-1]
	return item
}

func (h *OrderHeap) Peek() *shared.Order {
	if len(h.orders) == 0 {
		return nil
	}
	return h.orders[0]
}

// ==============================================
// ORDER BOOK
// ==============================================

type OrderBook struct {
	Symbol           string
	BuyOrders        *OrderHeap
	SellOrders       *OrderHeap
	mutex            sync.RWMutex
	MaxOrdersPerSide int
	TotalOrders      int
}

func (ob *OrderBook) Match(order *shared.Order, obm *OrderBookManager) ([]*shared.Trade, []*shared.Order) {
	var trades []*shared.Trade
	var ordersToUpdate []*shared.Order

	if order.TimeInForce == shared.FOK && order.OrderType == shared.LIMIT && !ob.canFillCompletely(order) {
		order.OrderStatus = shared.CANCELLED
		return trades, ordersToUpdate
	}

	switch order.Side {
	case shared.BUY:
		trades, ordersToUpdate = ob.matchBuyOrder(order, obm)
	case shared.SELL:
		trades, ordersToUpdate = ob.matchSellOrder(order, obm)
	default:
		order.OrderStatus = shared.CANCELLED
	}
	return trades, ordersToUpdate
}

func (ob *OrderBook) matchBuyOrder(order *shared.Order, obm *OrderBookManager) ([]*shared.Trade, []*shared.Order) {
	trades := make([]*shared.Trade, 0, 10)
	ordersToUpdate := make([]*shared.Order, 0, 10)

	for ob.SellOrders.Len() > 0 && order.RemainingQty > 0 {
		sellOrder := ob.SellOrders.Peek()

		if order.OrderType == shared.LIMIT && order.Price < sellOrder.Price {
			break
		}

		fillQty := min(order.RemainingQty, sellOrder.RemainingQty)

		trade := &shared.Trade{
			ID:          uuid.New().String(),
			Symbol:      order.Symbol,
			Price:       sellOrder.Price,
			Qty:         fillQty,
			BuyOrderID:  order.ID,
			SellOrderID: sellOrder.ID,
			MakerSide:   string(shared.SELL),
			Timestamp:   time.Now(),
			BuyUserID:   order.UserID,
			SellUserID:  sellOrder.UserID,
		}
		trades = append(trades, trade)

		order.RemainingQty -= fillQty
		sellOrder.RemainingQty -= fillQty

		if sellOrder.RemainingQty == 0 {
			sellOrder.OrderStatus = shared.COMPLETE
			heap.Pop(ob.SellOrders)
			ordersToUpdate = append(ordersToUpdate, sellOrder)
			ob.TotalOrders--
		} else {
			sellOrder.OrderStatus = shared.PARTIALLY_FILLED
			ordersToUpdate = append(ordersToUpdate, sellOrder)
		}
	}

	if err := ob.handleRemainingQuantity(order, obm); err != nil {
		log.Printf("Warning: %v", err)
	}
	return trades, ordersToUpdate
}

func (ob *OrderBook) matchSellOrder(order *shared.Order, obm *OrderBookManager) ([]*shared.Trade, []*shared.Order) {
	trades := make([]*shared.Trade, 0, 10)
	ordersToUpdate := make([]*shared.Order, 0, 10)

	for ob.BuyOrders.Len() > 0 && order.RemainingQty > 0 {
		buyOrder := ob.BuyOrders.Peek()

		if order.OrderType == shared.LIMIT && order.Price > buyOrder.Price {
			break
		}

		fillQty := min(order.RemainingQty, buyOrder.RemainingQty)

		trade := &shared.Trade{
			ID:          uuid.New().String(),
			Symbol:      order.Symbol,
			Price:       buyOrder.Price,
			Qty:         fillQty,
			BuyOrderID:  buyOrder.ID,
			SellOrderID: order.ID,
			MakerSide:   string(shared.BUY),
			Timestamp:   time.Now(),
			BuyUserID:   buyOrder.UserID,
			SellUserID:  order.UserID,
		}
		trades = append(trades, trade)

		order.RemainingQty -= fillQty
		buyOrder.RemainingQty -= fillQty

		if buyOrder.RemainingQty == 0 {
			buyOrder.OrderStatus = shared.COMPLETE
			heap.Pop(ob.BuyOrders)
			ordersToUpdate = append(ordersToUpdate, buyOrder)
			ob.TotalOrders--
		} else {
			buyOrder.OrderStatus = shared.PARTIALLY_FILLED
			ordersToUpdate = append(ordersToUpdate, buyOrder)
		}
	}

	if err := ob.handleRemainingQuantity(order, obm); err != nil {
		log.Printf("Warning: %v", err)
	}
	return trades, ordersToUpdate
}

func (ob *OrderBook) handleRemainingQuantity(order *shared.Order, obm *OrderBookManager) error {
	if order.RemainingQty == 0 {
		order.OrderStatus = shared.COMPLETE
		return nil
	}

	if order.OrderType == shared.MARKET {
		order.OrderStatus = shared.COMPLETE
		return nil
	}

	switch order.TimeInForce {
	case shared.GTC:
		if err := ob.addToBook(order); err != nil {
			log.Printf("Order book full, completing order: %s", err)
			order.OrderStatus = shared.CANCELLED
			return err
		}
	case shared.IOC, shared.FOK:
		order.OrderStatus = shared.COMPLETE
	}
	return nil
}

func (ob *OrderBook) canFillCompletely(order *shared.Order) bool {
	var availableQty float64

	switch order.Side {
	case shared.BUY:
		for _, sellOrder := range ob.SellOrders.orders {
			if order.OrderType == shared.LIMIT && order.Price < sellOrder.Price {
				break
			}
			availableQty += sellOrder.RemainingQty
			if availableQty >= order.RemainingQty {
				return true
			}
		}
	case shared.SELL:
		for _, buyOrder := range ob.BuyOrders.orders {
			if order.OrderType == shared.LIMIT && order.Price > buyOrder.Price {
				break
			}
			availableQty += buyOrder.RemainingQty
			if availableQty >= order.RemainingQty {
				return true
			}
		}
	}

	return false
}

func (ob *OrderBook) addToBook(order *shared.Order) error {
	if ob.TotalOrders >= MAX_TOTAL_ORDERS {
		order.OrderStatus = shared.CANCELLED
		return fmt.Errorf("order book full: %d total orders for symbol %s", MAX_TOTAL_ORDERS, ob.Symbol)
	}

	if order.Side == shared.BUY {
		if ob.BuyOrders.Len() >= ob.MaxOrdersPerSide {
			order.OrderStatus = shared.CANCELLED
			return fmt.Errorf("buy side full: %d orders for symbol %s", ob.MaxOrdersPerSide, ob.Symbol)
		}
		heap.Push(ob.BuyOrders, order)
	} else {
		if ob.SellOrders.Len() >= ob.MaxOrdersPerSide {
			order.OrderStatus = shared.CANCELLED
			return fmt.Errorf("sell side full: %d orders for symbol %s", ob.MaxOrdersPerSide, ob.Symbol)
		}
		heap.Push(ob.SellOrders, order)
	}

	ob.TotalOrders++
	order.OrderStatus = shared.PENDING

	if ob.TotalOrders > MAX_TOTAL_ORDERS*0.8 {
		log.Printf("WARNING: Order book %s at %d orders (80%% capacity)", ob.Symbol, ob.TotalOrders)
	}

	return nil
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

// ==============================================
// ORDER STATUS MANAGER
// ==============================================

type OrderStatusManager struct {
	redisClient *redis.Client
	kafkaWriter *kafka.Writer
}

func NewOrderStatusManager(redisClient *redis.Client, kafkaWriter *kafka.Writer) *OrderStatusManager {
	return &OrderStatusManager{
		redisClient: redisClient,
		kafkaWriter: kafkaWriter,
	}
}

func (osm *OrderStatusManager) UpdateOrderStatus(order *shared.Order, newStatus shared.OrderStatus, message string) error {
	ctx := context.Background()
	orderKey := fmt.Sprintf("order_status:%s", order.ID)
	statusKey := fmt.Sprintf("order_status:%s:history", order.ID)

	now := time.Now()

	pipe := osm.redisClient.Pipeline()
	pipe.HMSet(ctx, orderKey, map[string]interface{}{
		"order_id":      order.ID,
		"user_id":       order.UserID,
		"symbol":        order.Symbol,
		"side":          string(order.Side),
		"order_type":    string(order.OrderType),
		"original_qty":  fmt.Sprintf("%.8f", order.OriginalQty),
		"remaining_qty": fmt.Sprintf("%.8f", order.RemainingQty),
		"price":         fmt.Sprintf("%.8f", order.Price),
		"status":        string(newStatus),
		"created_at":    fmt.Sprintf("%d", order.Timestamp.Unix()),
		"updated_at":    fmt.Sprintf("%d", now.Unix()),
		"last_message":  message,
	})

	statusEntry := fmt.Sprintf("%d:%s:%s", now.Unix(), string(newStatus), message)
	pipe.LPush(ctx, statusKey, statusEntry)
	pipe.LTrim(ctx, statusKey, 0, 99)

	pipe.Expire(ctx, orderKey, 24*time.Hour)
	pipe.Expire(ctx, statusKey, 24*time.Hour)

	_, err := pipe.Exec(ctx)
	return err
}

func (osm *OrderStatusManager) GetOrderHistory(orderID string) (*shared.OrderHistory, error) {
	ctx := context.Background()
	key := fmt.Sprintf("order_status:%s", orderID)

	orderData, err := osm.redisClient.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(orderData) == 0 {
		return nil, fmt.Errorf("order not found")
	}

	originalQty, _ := strconv.ParseFloat(orderData["original_qty"], 64)
	remainingQty, _ := strconv.ParseFloat(orderData["remaining_qty"], 64)
	price, _ := strconv.ParseFloat(orderData["price"], 64)
	createdAtUnix, _ := strconv.ParseInt(orderData["created_at"], 10, 64)
	updatedAtUnix, _ := strconv.ParseInt(orderData["updated_at"], 10, 64)

	orderHistory := &shared.OrderHistory{
		OrderID:       orderData["order_id"],
		UserID:        orderData["user_id"],
		Symbol:        orderData["symbol"],
		Side:          shared.Side(orderData["side"]),
		OrderType:     shared.OrderType(orderData["order_type"]),
		OriginalQty:   originalQty,
		RemainingQty:  remainingQty,
		Price:         price,
		StatusHistory: []shared.OrderStatusDetail{},
		CreatedAt:     time.Unix(createdAtUnix, 0),
		UpdatedAt:     time.Unix(updatedAtUnix, 0),
	}

	statusKey := fmt.Sprintf("order_status:%s:history", orderID)
	statusEntries, err := osm.redisClient.LRange(ctx, statusKey, 0, -1).Result()
	if err == nil {
		for _, entry := range statusEntries {
			parts := strings.SplitN(entry, ":", 3)
			if len(parts) == 3 {
				timestamp, _ := strconv.ParseInt(parts[0], 10, 64)
				statusDetail := shared.OrderStatusDetail{
					Status:    shared.OrderStatus(parts[1]),
					Timestamp: time.Unix(timestamp, 0),
					Message:   parts[2],
				}
				orderHistory.StatusHistory = append(orderHistory.StatusHistory, statusDetail)
			}
		}
	}
	return orderHistory, nil
}

// ==============================================
// REDIS PERSISTENCE FUNCTIONS
// ==============================================

func (obm *OrderBookManager) addOrderToRedis(order *shared.Order) error {
	ctx := context.Background()

	key := fmt.Sprintf("orderbook:%s:%s", order.Symbol, order.Side)

	var score float64
	if order.Side == shared.BUY {
		score = order.Price
	} else {
		score = -order.Price
	}

	z := redis.Z{
		Score:  score,
		Member: order.ID,
	}

	return obm.redisClient.ZAdd(ctx, key, z).Err()
}

func (obm *OrderBookManager) removeOrderFromRedis(symbol, side, orderID string) error {
	ctx := context.Background()
	key := fmt.Sprintf("orderbook:%s:%s", symbol, side)

	return obm.redisClient.ZRem(ctx, key, orderID).Err()
}

func (obm *OrderBookManager) restoreOrderBookFromRedis(book *OrderBook) error {
	ctx := context.Background()
	buyKey := fmt.Sprintf("orderbook:%s:buy", book.Symbol)
	sellKey := fmt.Sprintf("orderbook:%s:sell", book.Symbol)

	buyData, err := obm.redisClient.Get(ctx, buyKey).Result()
	if err == nil {
		var buyOrders []*shared.Order
		json.Unmarshal([]byte(buyData), &buyOrders)
		for _, order := range buyOrders {
			heap.Push(book.BuyOrders, order)
		}
	}

	sellData, err := obm.redisClient.Get(ctx, sellKey).Result()
	if err == nil {
		var sellOrders []*shared.Order
		json.Unmarshal([]byte(sellData), &sellOrders)
		for _, order := range sellOrders {
			heap.Push(book.SellOrders, order)
		}
	}

	return nil
}

func (obm *OrderBookManager) publishTradeEvent(trade *shared.Trade) error {
	event := shared.TradeEvent{
		Type:      "EXECUTION",
		Trade:     trade,
		Timestamp: time.Now(),
	}

	data, _ := json.Marshal(event)
	message := kafka.Message{
		Key:   []byte(trade.Symbol),
		Value: data,
	}

	return obm.kafkaWriter.WriteMessages(context.Background(), message)
}

func (obm *OrderBookManager) storeTradeInRedis(trade *shared.Trade) error {
	ctx := context.Background()
	tradeKey := fmt.Sprintf("trade:%s", trade.ID)
	tradesListKey := fmt.Sprintf("trades:%s", trade.Symbol)

	pipe := obm.redisClient.Pipeline()

	pipe.HMSet(ctx, tradeKey, map[string]interface{}{
		"symbol":        trade.Symbol,
		"price":         fmt.Sprintf("%.8f", trade.Price),
		"qty":           fmt.Sprintf("%.8f", trade.Qty),
		"buy_order_id":  trade.BuyOrderID,
		"sell_order_id": trade.SellOrderID,
		"maker_side":    trade.MakerSide,
		"timestamp":     fmt.Sprintf("%d", trade.Timestamp.Unix()),
		"buy_user_id":   trade.BuyUserID,
		"sell_user_id":  trade.SellUserID,
	})

	pipe.LPush(ctx, tradesListKey, trade.ID)
	pipe.LTrim(ctx, tradesListKey, 0, 999)
	pipe.SAdd(ctx, "symbols", trade.Symbol)

	pipe.Expire(ctx, tradeKey, 7*24*time.Hour)
	pipe.Expire(ctx, tradesListKey, 7*24*time.Hour)

	_, err := pipe.Exec(ctx)
	return err
}

func (obm *OrderBookManager) getTradeFromRedis(tradeID string) (*shared.Trade, error) {
	ctx := context.Background()
	tradeKey := fmt.Sprintf("trade:%s", tradeID)

	tradeData, err := obm.redisClient.HGetAll(ctx, tradeKey).Result()
	if err != nil || len(tradeData) == 0 {
		return nil, fmt.Errorf("trade not found: %s", tradeID)
	}

	price, _ := strconv.ParseFloat(tradeData["price"], 64)
	qty, _ := strconv.ParseFloat(tradeData["qty"], 64)
	timestampUnix, _ := strconv.ParseInt(tradeData["timestamp"], 10, 64)

	trade := &shared.Trade{
		ID:          tradeID,
		Symbol:      tradeData["symbol"],
		Price:       price,
		Qty:         qty,
		BuyOrderID:  tradeData["buy_order_id"],
		SellOrderID: tradeData["sell_order_id"],
		MakerSide:   tradeData["maker_side"],
		Timestamp:   time.Unix(timestampUnix, 0),
		BuyUserID:   tradeData["buy_user_id"],
		SellUserID:  tradeData["sell_user_id"],
	}

	return trade, nil
}
