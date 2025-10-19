package shared

import "time"

type Side string
type OrderType string
type TimeInForce string
type OrderStatus string

const (
	BUY  Side = "BUY"
	SELL Side = "SELL"
)

const (
	LIMIT  OrderType = "LIMIT"
	MARKET OrderType = "MARKET"
)

const (
	GTC TimeInForce = "GTC"
	IOC TimeInForce = "IOC"
	FOK TimeInForce = "FOK"
)

const (
	COMPLETE         OrderStatus = "COMPLETE"
	PENDING          OrderStatus = "PENDING"
	CANCELLED        OrderStatus = "CANCELLED"
	PARTIALLY_FILLED OrderStatus = "PARTIALLY_FILLED"
)

const (
	ORDER_TOPIC        = "order-events"
	TRADE_TOPIC        = "trade-events"
	NOTIFICATION_TOPIC = "notification-events"
)

type Order struct {
	ID           string      `json:"id"`
	Symbol       string      `json:"symbol"`
	Side         Side        `json:"side"`
	Price        float64     `json:"price"`
	OrderType    OrderType   `json:"order_type"`
	OrderStatus  OrderStatus `json:"order_status"`
	TimeInForce  TimeInForce `json:"time_in_force"`
	OriginalQty  float64     `json:"original_qty"`
	RemainingQty float64     `json:"remaining_qty"`
	Timestamp    time.Time   `json:"timestamp"`
	UserID       string      `json:"user_id"`
}

type Trade struct {
	ID          string    `json:"id"`
	Symbol      string    `json:"symbol"`
	Qty         float64   `json:"qty"`
	Price       float64   `json:"price"`
	Timestamp   time.Time `json:"timestamp"`
	BuyOrderID  string    `json:"buy_order_id"`
	SellOrderID string    `json:"sell_order_id"`
	MakerSide   string    `json:"maker_side"`

	BuyUserID  string `json:"buy_user_id"`
	SellUserID string `json:"sell_user_id"`
}

type OrderEvent struct {
	Type      string    `json:"type"` // NEW, CANCEL, MODIFY
	Order     *Order    `json:"order"`
	Timestamp time.Time `json:"timestamp"`
}

type TradeEvent struct {
	Type      string    `json:"type"` // EXECUTION
	Trade     *Trade    `json:"trade"`
	Timestamp time.Time `json:"timestamp"`
}

type TradeNotification struct {
	UserID    string    `json:"user_id"`
	OrderID   string    `json:"order_id"`
	Symbol    string    `json:"symbol"`
	Side      string    `json:"side"`
	Quantity  float64   `json:"quantity"`
	Price     float64   `json:"price"`
	TradeID   string    `json:"trade_id"`
	Timestamp time.Time `json:"timestamp"`
	Status    string    `json:"status"` // "PARTIAL_FILL" or "COMPLETE_FILL"
}

type OrderStatusDetail struct {
	Status    OrderStatus `json:"status"`
	Timestamp time.Time   `json:"timestamp"`
	Message   string      `json:"message,omitempty"`
}

type OrderHistory struct {
	OrderID       string              `json:"order_id"`
	UserID        string              `json:"user_id"`
	Symbol        string              `json:"symbol"`
	Side          Side                `json:"side"`
	OrderType     OrderType           `json:"order_type"`
	OriginalQty   float64             `json:"original_qty"`
	RemainingQty  float64             `json:"remaining_qty"`
	Price         float64             `json:"price"`
	StatusHistory []OrderStatusDetail `json:"status_history"`
	CreatedAt     time.Time           `json:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at"`
}
