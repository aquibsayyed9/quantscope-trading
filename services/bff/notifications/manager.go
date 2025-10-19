//	{
//	 "type": "TRADE_EXECUTION",
//	 "user_id": "user123",
//	 "order_id": "abc-123",
//	 "message": "Order partially filled: 0.5 BTC at $50,000",
//	 "data": {
//	   "trade_id": "trade-456",
//	   "symbol": "BTCUSD",
//	   "side": "BUY",
//	   "executed_qty": 0.5,
//	   "executed_price": 50000,
//	   "remaining_qty": 1.0
//	 },
//	 "timestamp": "2025-09-20T10:00:00Z"
//	}
package notifications

import (
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type NotificationManager struct {
	connections map[string]*websocket.Conn
	mutex       sync.RWMutex
}

type Notification struct {
	Type      string                 `json:"type"`
	UserID    string                 `json:"user_id"`
	Message   string                 `json:"message"`
	Data      map[string]interface{} `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
}

func NewNotificationManager() *NotificationManager {
	return &NotificationManager{
		connections: make(map[string]*websocket.Conn),
	}
}

func (nm *NotificationManager) AddConnection(userID string, conn *websocket.Conn) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()

	if existingConn, exists := nm.connections[userID]; exists {
		existingConn.Close()
	}

	nm.connections[userID] = conn
	log.Printf("WebSocket connection added for user: %s", userID)
}

func (nm *NotificationManager) RemoveConnection(userID string) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()

	if conn, exists := nm.connections[userID]; exists {
		conn.Close()
		delete(nm.connections, userID)
		log.Printf("WebSocket connection removed for user: %s", userID)
	}
}

func (nm *NotificationManager) SendNotification(userID string, notification Notification) error {
	nm.mutex.RLock()
	conn, exists := nm.connections[userID]
	nm.mutex.RUnlock()

	if !exists {
		log.Printf("No WebSocket connection for user: %s", userID)
		return nil
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	err := conn.WriteJSON(notification)
	if err != nil {
		log.Printf("Failed to send notification to user %s: %v", userID, err)
		nm.RemoveConnection(userID)
		return err
	}

	log.Printf("Notification sent to user %s: %s", userID, notification.Type)
	return nil
}

func (nm *NotificationManager) BroadcastNotification(notification Notification) {
	nm.mutex.RLock()
	connections := make(map[string]*websocket.Conn)
	for userID, conn := range nm.connections {
		connections[userID] = conn
	}
	nm.mutex.RUnlock()

	for userID, conn := range connections {
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		err := conn.WriteJSON(notification)
		if err != nil {
			log.Printf("Failed to broadcast to user %s: %v", userID, err)
			nm.RemoveConnection(userID)
		}
	}
}

// GetConnectionCount returns the number of active connections
func (nm *NotificationManager) GetConnectionCount() int {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	return len(nm.connections)
}
