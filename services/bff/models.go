package main

type PlaceOrderRequest struct {
	Symbol      string  `json:"symbol" binding:"required"`
	Side        string  `json:"side" binding:"required"`
	Price       float64 `json:"price"`
	Quantity    float64 `json:"quantity" binding:"required"`
	OrderType   string  `json:"order_type" binding:"required"`
	TimeInForce string  `json:"time_in_force" binding:"required"`
	UserID      string  `json:"user_id" binding:"required"`
}

type PlaceOrderResponse struct {
	OrderID     string  `json:"order_id"`
	Symbol      string  `json:"symbol"`
	Side        string  `json:"side"`
	Quantity    float64 `json:"quantity"`
	Price       float64 `json:"price"`
	OrderType   string  `json:"order_type"`
	TimeInForce string  `json:"time_in_force"`
	Status      string  `json:"status"`
	Timestamp   string  `json:"timestamp"`
}

type CancelOrderResponse struct {
	OrderID   string `json:"order_id"`
	Symbol    string `json:"symbol"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}
