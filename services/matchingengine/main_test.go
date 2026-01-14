package main

import (
	"fmt"
	"go-exchange/shared"
	//"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	//"unsafe"
)

func TestRawMatchingOutput(t *testing.T) {

	// Create WAL
	wal, err := NewWAL("./test-wal", 4*1024*1024, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	//defer os.RemoveAll("./test-wal")

	obm := &OrderBookManager{
		books:      make(map[string]*OrderBook),
		maxSymbols: 100,
		wal:        wal,
	}

	//var buyOrders []*shared.Order
	var wg sync.WaitGroup
	var matchCount int32
	start := time.Now()

	fmt.Println("adding buy orders...")
	for i := range 100000 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			order := &shared.Order{
				ID:           fmt.Sprintf("BUY-%d", id),
				Symbol:       "BTC-USD",
				OrderType:    shared.LIMIT,
				Side:         shared.BUY,
				Price:        float64(50000),
				TimeInForce:  shared.GTC,
				OriginalQty:  1,
				RemainingQty: 1,
				OrderStatus:  shared.PENDING,
				Timestamp:    time.Now(),
			}
			//fmt.Println("size of order: ", unsafe.Sizeof(order))
			obm.ProcessOrder(order)
		}(i)

		//buyOrders = append(buyOrders, order)
	}
	wg.Wait()
	book, _ := obm.GetOrderBook("BTC-USD")
	fmt.Printf("After buys - Buy orders in book: %d\n", book.BuyOrders.Len())

	fmt.Println("adding sell orders...")
	for i := range 100000 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			order := &shared.Order{
				ID:           fmt.Sprintf("BUY-%d", id),
				Symbol:       "BTC-USD",
				OrderType:    shared.LIMIT,
				Side:         shared.SELL,
				Price:        float64(50000),
				TimeInForce:  shared.GTC,
				OriginalQty:  1,
				RemainingQty: 1,
				OrderStatus:  shared.PENDING,
				Timestamp:    time.Now(),
			}
			//obm.ProcessOrder(order)
			trades, _ := obm.ProcessOrder(order)
			if len(trades) > 0 {
				atomic.AddInt32(&matchCount, int32(len(trades)))
			}
		}(i)

		//buyOrders = append(buyOrders, order)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\nAfter sells - Buy orders remaining: %d\n", book.BuyOrders.Len())
	fmt.Printf("Sell orders remaining: %d\n", book.SellOrders.Len())
	fmt.Printf("Total trades generated: %d\n", matchCount)
	fmt.Printf("\n200000 orders in %v\n", elapsed)
	fmt.Printf("Throughput: %.0f orders/sec\n", 200000/elapsed.Seconds())

}
