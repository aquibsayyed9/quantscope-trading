package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	//"time"

	"go-exchange/shared"

	"github.com/segmentio/kafka-go"
)

type TradeConsumer struct {
	reader              *kafka.Reader
	notificationManager *NotificationManager
}

func NewTradeConsumer(kafkaBroker string, nm *NotificationManager) *TradeConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "trade-events",
		GroupID: "bff-notifications",
	})

	return &TradeConsumer{
		reader:              reader,
		notificationManager: nm,
	}
}

func (tc *TradeConsumer) Start(ctx context.Context) error {
	log.Println("Starting trade events consumer...")

	for {
		select {
		case <-ctx.Done():
			return tc.reader.Close()
		default:
			message, err := tc.reader.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading trade message: %v", err)
				continue
			}

			tc.processTrade(message.Value)
		}
	}
}

func (tc *TradeConsumer) processTrade(messageValue []byte) {
	var tradeEvent shared.TradeEvent
	if err := json.Unmarshal(messageValue, &tradeEvent); err != nil {
		log.Printf("Error unmarshaling trade event: %v", err)
		return
	}

	if tradeEvent.Type != "EXECUTION" {
		return
	}

	trade := tradeEvent.Trade
	if trade.BuyOrderID != "loadtest" && trade.SellUserID != "loadtest" {
		tc.notifyTradeParticipants(trade)
	}
}

func (tc *TradeConsumer) notifyTradeParticipants(trade *shared.Trade) {
	buyerNotification := Notification{
		Type:    "TRADE_EXECUTION",
		Message: fmt.Sprintf("Buy order executed: %.2f %s at %.2f", trade.Qty, trade.Symbol, trade.Price),
		Data: map[string]interface{}{
			"trade_id": trade.ID,
			"symbol":   trade.Symbol,
			"side":     "BUY",
			"quantity": trade.Qty,
			"price":    trade.Price,
		},
		Timestamp: trade.Timestamp,
	}

	sellerNotification := Notification{
		Type:    "TRADE_EXECUTION",
		Message: fmt.Sprintf("Sell order executed: %.2f %s at %.2f", trade.Qty, trade.Symbol, trade.Price),
		Data: map[string]interface{}{
			"trade_id": trade.ID,
			"symbol":   trade.Symbol,
			"side":     "SELL",
			"quantity": trade.Qty,
			"price":    trade.Price,
		},
		Timestamp: trade.Timestamp,
	}

	// For now, just log since we don't have user IDs yet
	log.Printf("Trade notification ready: %s", buyerNotification.Message)
	log.Printf("Trade notification ready: %s", sellerNotification.Message)

	tc.notificationManager.SendNotification(trade.BuyUserID, buyerNotification)
	tc.notificationManager.SendNotification(trade.SellUserID, sellerNotification)
}
