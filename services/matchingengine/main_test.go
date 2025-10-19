package main

import (
	"fmt"
	"testing"
	"time"

	"go-exchange/shared"

	"github.com/google/uuid"
)

// Mock implementations - no external dependencies
type MockRedisClient struct{}

func (m *MockRedisClient) Set(ctx interface{}, key string, value interface{}, expiration time.Duration) error {
	return nil // Mock success
}

func (m *MockRedisClient) Get(ctx interface{}, key string) MockResult {
	return MockResult{}
}

func (m *MockRedisClient) Pipeline() MockPipeline {
	return MockPipeline{}
}

func (m *MockRedisClient) Close() error {
	return nil
}

type MockResult struct{}

func (m MockResult) Result() (string, error) {
	return "", fmt.Errorf("key not found") // Simulate empty cache
}

type MockPipeline struct{}

func (m MockPipeline) Set(ctx interface{}, key string, value interface{}, expiration time.Duration) {}
func (m MockPipeline) Exec(ctx interface{}) ([]interface{}, error) {
	return nil, nil
}

type MockKafkaWriter struct{}

func (m *MockKafkaWriter) WriteMessages(ctx interface{}, msgs ...interface{}) error {
	return nil // Mock success
}

func (m *MockKafkaWriter) Close() error {
	return nil
}

// Create a testable OrderBookManager that doesn't need Redis/Kafka
func NewTestOrderBookManager() *TestOrderBookManager {
	return &TestOrderBookManager{
		books:      make(map[string]*OrderBook),
		maxSymbols: MAX_SYMBOLS,
	}
}

type TestOrderBookManager struct {
	books      map[string]*OrderBook
	maxSymbols int
}

func (tobm *TestOrderBookManager) GetOrderBook(symbol string) (*OrderBook, error) {
	if book, exists := tobm.books[symbol]; exists {
		return book, nil
	}

	// Check symbol limit
	if len(tobm.books) >= tobm.maxSymbols {
		return nil, fmt.Errorf("maximum symbols reached: %d/%d", len(tobm.books), tobm.maxSymbols)
	}

	book := &OrderBook{
		Symbol:           symbol,
		BuyOrders:        make([]*shared.Order, 0, 1000),
		SellOrders:       make([]*shared.Order, 0, 1000),
		MaxOrdersPerSide: MAX_ORDERS_PER_SIDE,
		TotalOrders:      0,
	}

	tobm.books[symbol] = book
	return book, nil
}

func (tobm *TestOrderBookManager) ProcessOrder(order *shared.Order) ([]*shared.Trade, error) {
	book, err := tobm.GetOrderBook(order.Symbol)
	if err != nil {
		order.OrderStatus = shared.CANCELLED
		return nil, fmt.Errorf("failed to get order book: %w", err)
	}

	trades := book.Match(order, tobm)
	// No Redis/Kafka operations in test version
	return trades, nil
}

// Unit Tests - No external dependencies
func TestOrderBookLimitsUnit(t *testing.T) {
	t.Run("Normal Order Processing", func(t *testing.T) {
		tobm := NewTestOrderBookManager()

		order := &shared.Order{
			ID:           uuid.New().String(),
			Symbol:       "TESTBTC",
			Side:         shared.BUY,
			Price:        50000,
			OrderType:    shared.LIMIT,
			TimeInForce:  shared.GTC,
			OriginalQty:  1.0,
			RemainingQty: 1.0,
			Timestamp:    time.Now(),
		}

		trades, err := tobm.ProcessOrder(order)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}

		if len(trades) != 0 {
			t.Errorf("Expected 0 trades (no matching orders), got %d", len(trades))
		}

		if order.OrderStatus != shared.PENDING {
			t.Errorf("Expected order status PENDING, got %s", order.OrderStatus)
		}

		t.Log("✅ Normal order processing test passed")
	})

	t.Run("Symbol Limit Enforcement", func(t *testing.T) {
		tobm := NewTestOrderBookManager()
		tobm.maxSymbols = 2 // Set low limit for testing

		symbolLimitReached := false

		for i := 0; i < 5; i++ {
			symbol := fmt.Sprintf("LIMIT_%d", i)
			order := &shared.Order{
				ID:           uuid.New().String(),
				Symbol:       symbol,
				Side:         shared.BUY,
				Price:        100,
				OrderType:    shared.LIMIT,
				TimeInForce:  shared.GTC,
				OriginalQty:  1.0,
				RemainingQty: 1.0,
				Timestamp:    time.Now(),
			}

			_, err := tobm.ProcessOrder(order)

			if err != nil {
				t.Logf("✅ Symbol limit enforced at symbol %d: %v", i, err)

				if order.OrderStatus != shared.CANCELLED {
					t.Errorf("Expected order status CANCELLED, got %s", order.OrderStatus)
				}

				symbolLimitReached = true
				break
			}

			t.Logf("Created symbol %s, total: %d", symbol, len(tobm.books))
		}

		if !symbolLimitReached {
			t.Errorf("❌ Symbol limit was not enforced! Final count: %d", len(tobm.books))
		}

		// Verify exactly 2 symbols were created
		if len(tobm.books) != 2 {
			t.Errorf("Expected 2 symbols, got %d", len(tobm.books))
		}

		t.Log("✅ Symbol limit enforcement test passed")
	})

	t.Run("Order Count Limit", func(t *testing.T) {
		book := &OrderBook{
			Symbol:           "LIMITTEST",
			BuyOrders:        make([]*shared.Order, 0, 10),
			SellOrders:       make([]*shared.Order, 0, 10),
			MaxOrdersPerSide: 3,
			TotalOrders:      0,
		}

		orderLimitReached := false

		for i := 0; i < 6; i++ {
			order := &shared.Order{
				ID:           fmt.Sprintf("order-%d", i),
				Symbol:       "LIMITTEST",
				Side:         shared.BUY,
				Price:        float64(50000 + i),
				OrderType:    shared.LIMIT,
				TimeInForce:  shared.GTC,
				OriginalQty:  1.0,
				RemainingQty: 1.0,
				Timestamp:    time.Now(),
			}

			err := book.addToBook(order)

			if err != nil {
				t.Logf("✅ Order limit enforced at order %d: %v", i, err)

				if order.OrderStatus != shared.CANCELLED {
					t.Errorf("Expected order status CANCELLED, got %s", order.OrderStatus)
				}

				orderLimitReached = true
				break
			}
		}

		if !orderLimitReached {
			t.Error("❌ Order count limit was not enforced!")
		}

		// Verify exactly 3 orders were added
		if len(book.BuyOrders) != 3 {
			t.Errorf("Expected 3 buy orders, got %d", len(book.BuyOrders))
		}

		if book.TotalOrders != 3 {
			t.Errorf("Expected 3 total orders, got %d", book.TotalOrders)
		}

		t.Log("✅ Order count limit test passed")
	})
}

func TestOrderMatching(t *testing.T) {
	t.Run("Basic Buy-Sell Match", func(t *testing.T) {
		book := &OrderBook{
			Symbol:           "MATCHTEST",
			BuyOrders:        make([]*shared.Order, 0),
			SellOrders:       make([]*shared.Order, 0),
			MaxOrdersPerSide: 1000,
			TotalOrders:      0,
		}

		// Add a sell order first
		sellOrder := &shared.Order{
			ID:           "sell-1",
			Symbol:       "MATCHTEST",
			Side:         shared.SELL,
			Price:        100,
			OrderType:    shared.LIMIT,
			TimeInForce:  shared.GTC,
			OriginalQty:  5.0,
			RemainingQty: 5.0,
			Timestamp:    time.Now(),
		}

		book.addToBook(sellOrder)

		// Add a matching buy order
		buyOrder := &shared.Order{
			ID:           "buy-1",
			Symbol:       "MATCHTEST",
			Side:         shared.BUY,
			Price:        100,
			OrderType:    shared.LIMIT,
			TimeInForce:  shared.GTC,
			OriginalQty:  3.0,
			RemainingQty: 3.0,
			Timestamp:    time.Now(),
		}

		trades := book.Match(buyOrder)

		// Verify trade was created
		if len(trades) != 1 {
			t.Errorf("Expected 1 trade, got %d", len(trades))
		}

		if len(trades) > 0 {
			trade := trades[0]
			if trade.Qty != 3.0 {
				t.Errorf("Expected trade quantity 3.0, got %f", trade.Qty)
			}
			if trade.Price != 100.0 {
				t.Errorf("Expected trade price 100.0, got %f", trade.Price)
			}
		}

		// Verify order states
		if buyOrder.RemainingQty != 0 {
			t.Errorf("Expected buy order fully filled, remaining: %f", buyOrder.RemainingQty)
		}

		if sellOrder.RemainingQty != 2.0 {
			t.Errorf("Expected sell order partially filled (2.0 remaining), got: %f", sellOrder.RemainingQty)
		}

		t.Log("✅ Basic matching test passed")
	})

	t.Run("FOK Order Test", func(t *testing.T) {
		book := &OrderBook{
			Symbol:           "FOKTEST",
			BuyOrders:        make([]*shared.Order, 0),
			SellOrders:       make([]*shared.Order, 0),
			MaxOrdersPerSide: 1000,
			TotalOrders:      0,
		}

		// Add small sell order
		sellOrder := &shared.Order{
			ID:           "sell-fok",
			Symbol:       "FOKTEST",
			Side:         shared.SELL,
			Price:        100,
			OrderType:    shared.LIMIT,
			TimeInForce:  shared.GTC,
			OriginalQty:  2.0,
			RemainingQty: 2.0,
			Timestamp:    time.Now(),
		}

		book.addToBook(sellOrder)

		// FOK order that cannot be completely filled
		fokOrder := &shared.Order{
			ID:           "buy-fok",
			Symbol:       "FOKTEST",
			Side:         shared.BUY,
			Price:        100,
			OrderType:    shared.LIMIT,
			TimeInForce:  shared.FOK,
			OriginalQty:  5.0, // More than available
			RemainingQty: 5.0,
			Timestamp:    time.Now(),
		}

		trades := book.Match(fokOrder)

		// FOK should be cancelled, no trades
		if len(trades) != 0 {
			t.Errorf("Expected 0 trades for unfillable FOK, got %d", len(trades))
		}

		if fokOrder.OrderStatus != shared.CANCELLED {
			t.Errorf("Expected FOK order to be CANCELLED, got %s", fokOrder.OrderStatus)
		}

		t.Log("✅ FOK order test passed")
	})
}

func TestMatchingEngineInitUnit(t *testing.T) {
	// This doesn't actually connect to Redis/Kafka
	tobm := NewTestOrderBookManager()

	if tobm == nil {
		t.Error("Failed to create test order book manager")
	}

	if tobm.books == nil {
		t.Error("Order books map not initialized")
	}

	t.Log("✅ Unit test initialization passed")
}
