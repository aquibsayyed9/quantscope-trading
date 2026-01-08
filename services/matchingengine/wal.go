package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"time"

	"go-exchange/shared"
)

type WAL struct {
	file     *os.File
	writer   *bufio.Writer
	mutex    sync.Mutex
	sequence uint64

	flushTicker *time.Ticker
	stopFlush   chan struct{}
	flushWg     sync.WaitGroup

	entriesWritten uint64
	bytesWritten   uint64
}

const (
	EntryTypeOrder = 1
	EntryTypeTrade = 2
)

func NewWAL(fileName string, bufferSize int, flushInterval time.Duration) (*WAL, error) {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	wal := &WAL{
		file:        file,
		writer:      bufio.NewWriterSize(file, bufferSize),
		sequence:    0,
		flushTicker: time.NewTicker(flushInterval),
		stopFlush:   make(chan struct{}),
	}

	// start background flusher
	wal.flushWg.Add(1)
	go wal.backgroundFlusher()

	return wal, nil
}

func (w *WAL) backgroundFlusher() {
	defer w.flushWg.Done()

	for {
		select {
		case <-w.stopFlush:
			return
		case <-w.flushTicker.C:
			w.Flush()
		}
	}
}

func (w *WAL) Flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return nil
}

func (w *WAL) Close() error {
	close(w.stopFlush)
	w.flushTicker.Stop()
	w.flushWg.Wait()

	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync: %w", err)
	}

	return w.file.Close()
}

func (w *WAL) AppendOrder(order *shared.Order) error {
	// [length:4][sequence:8][type:1][payload][checksum:4]
	data := serializeOrder(order)
	return w.WriteEntry(EntryTypeOrder, data)
}

func (w *WAL) AppendTrade(trade *shared.Trade) error {
	data := serializeTrade(trade)
	return w.WriteEntry(EntryTypeTrade, data)
}

func (w *WAL) WriteEntry(entryType byte, payload []byte) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.sequence++
	seq := w.sequence

	checksum := calculateChecksum(payload)

	// write header [length:4][sequence:8][type:1]
	headerSize := 13
	totalLength := uint32(len(payload) + headerSize + 4) // 4 bits for checksum

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], totalLength)
	binary.LittleEndian.PutUint64(header[4:12], seq)
	header[12] = entryType

	// header + payload + checksum
	if _, err := w.writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	if _, err := w.writer.Write(payload); err != nil {
		return fmt.Errorf("failed to write payload: %w", err)
	}

	checksumBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(checksumBytes, checksum)
	if _, err := w.writer.Write(checksumBytes); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	return nil
}

// simple checksum (FNV-1a hash)
func calculateChecksum(data []byte) uint32 {
	hash := uint32(2166136261)
	for _, b := range data {
		hash ^= uint32(b)
		hash *= 16777619
	}
	return hash
}

func serializeTrade(trade *shared.Trade) []byte {
	buf := make([]byte, 0, 256)

	// timestamp 8 bytes
	ts := make([]byte, 8)
	binary.LittleEndian.PutUint64(ts, uint64(trade.Timestamp.UnixNano()))
	buf = append(buf, ts...)

	// trade id
	buf = append(buf, byte(len(trade.ID)))
	buf = append(buf, []byte(trade.ID)...)

	// symbol
	buf = append(buf, byte(len(trade.Symbol)))
	buf = append(buf, []byte(trade.Symbol)...)

	// buy order id
	buf = append(buf, byte(len(trade.BuyOrderID)))
	buf = append(buf, []byte(trade.BuyOrderID)...)

	// sell order id
	buf = append(buf, byte(len(trade.SellOrderID)))
	buf = append(buf, []byte(trade.SellOrderID)...)

	// price 8 bytes
	priceBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(priceBytes, uint64(trade.Price*100000000))
	buf = append(buf, priceBytes...)

	// qty 8 bytes
	qtyBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(qtyBytes, uint64(trade.Qty*100000000))
	buf = append(buf, qtyBytes...)

	// maker side 1 byte
	buf = append(buf, sideToByte(trade.MakerSide))

	return buf
}

func deserializeTrade(data []byte) *shared.Trade {
	trade := &shared.Trade{}
	pos := 0

	// timestamp 8 bytes
	ts := binary.LittleEndian.Uint64(data[pos : pos+8])
	trade.Timestamp = time.Unix(0, int64(ts))
	pos += 8

	// trade id
	idLen := int(data[pos])
	pos++
	trade.ID = string(data[pos : pos+idLen])
	pos += idLen

	// symbol
	symbolLen := int(data[pos])
	pos++
	trade.Symbol = string(data[pos : pos+symbolLen])
	pos += symbolLen

	// buyorderid
	buyOrderIdLen := int(data[pos])
	pos++
	trade.BuyOrderID = string(data[pos : pos+buyOrderIdLen])
	pos += buyOrderIdLen

	// sellorderid
	sellOrderIdLen := int(data[pos])
	pos++
	trade.SellOrderID = string(data[pos : pos+sellOrderIdLen])
	pos += sellOrderIdLen

	// price 8 bytes
	price := binary.LittleEndian.Uint64(data[pos : pos+8])
	trade.Price = float64(price) / 100000000
	pos += 8

	// qty 8 bytes
	qty := binary.LittleEndian.Uint64(data[pos : pos+8])
	trade.Qty = float64(qty) / 100000000
	pos += 8

	// side 1 byte
	trade.MakerSide = byteToSide(data[pos])
	pos++

	return trade
}

func serializeOrder(order *shared.Order) []byte {
	buf := make([]byte, 0, 256)

	// timestamp 8 bytes
	ts := make([]byte, 8)
	binary.LittleEndian.PutUint64(ts, uint64(order.Timestamp.UnixNano()))

	buf = append(buf, ts...)

	// order id
	buf = append(buf, byte(len(order.ID)))
	buf = append(buf, []byte(order.ID)...)

	// symbol
	buf = append(buf, byte(len(order.Symbol)))
	buf = append(buf, []byte(order.Symbol)...)

	// side 1 byte
	buf = append(buf, sideToByte(order.Side))

	// order type 1 byte
	buf = append(buf, orderTypeToByte(order.OrderType))

	// time in force 1 byte
	buf = append(buf, timeInForceToByte(order.TimeInForce))

	// price 8 bytes
	priceBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(priceBytes, uint64(order.Price*100000000))
	buf = append(buf, priceBytes...)

	// qty 8 bytes
	qtyBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(qtyBytes, uint64(order.OriginalQty*100000000))
	buf = append(buf, qtyBytes...)

	// remaining qty 8 bytes
	remQtyBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(remQtyBytes, uint64(order.RemainingQty*100000000))
	buf = append(buf, remQtyBytes...)

	// order status 1 byte
	buf = append(buf, orderStatusToByte(order.OrderStatus))

	return buf
}

func deserializeOrder(data []byte) *shared.Order {
	order := &shared.Order{}
	pos := 0

	// timestamp 8 bytes
	ts := binary.LittleEndian.Uint64(data[pos : pos+8])
	order.Timestamp = time.Unix(0, int64(ts))
	pos += 8

	// order id
	idLen := int(data[pos])
	pos++
	order.ID = string(data[pos : pos+idLen])
	pos += idLen

	// symbol
	symbolLen := int(data[pos])
	pos++
	order.Symbol = string(data[pos : pos+symbolLen])
	pos += symbolLen

	// side 1 byte
	order.Side = byteToSide(data[pos])
	pos++

	// order type 1 byte
	order.OrderType = byteToOrderType(data[pos])
	pos++

	// time in force 1 byte
	order.TimeInForce = byteToTimeInForce(data[pos])
	pos++

	// price 8 bytes
	price := binary.LittleEndian.Uint64(data[pos : pos+8])
	order.Price = float64(price) / 100000000
	pos += 8

	// qty 8 bytes
	qty := binary.LittleEndian.Uint64(data[pos : pos+8])
	order.OriginalQty = float64(qty) / 100000000
	pos += 8

	// remaining qty 8 bytes
	remQty := binary.LittleEndian.Uint64(data[pos : pos+8])
	order.RemainingQty = float64(remQty) / 100000000
	pos += 8

	// order status 1 byte
	order.OrderStatus = byteToOrderStatus(data[pos])
	pos++

	return order
}

// Convert enums to bytes for serialization
func sideToByte(side shared.Side) byte {
	switch side {
	case shared.BUY:
		return 1
	case shared.SELL:
		return 2
	default:
		return 0
	}
}

func orderTypeToByte(orderType shared.OrderType) byte {
	switch orderType {
	case shared.LIMIT:
		return 1
	case shared.MARKET:
		return 2
	default:
		return 0
	}
}

func timeInForceToByte(tif shared.TimeInForce) byte {
	switch tif {
	case shared.GTC:
		return 1
	case shared.IOC:
		return 2
	case shared.FOK:
		return 3
	default:
		return 0
	}
}

func orderStatusToByte(status shared.OrderStatus) byte {
	switch status {
	case shared.PENDING:
		return 1
	case shared.PARTIALLY_FILLED:
		return 2
	case shared.COMPLETE:
		return 3
	case shared.CANCELLED:
		return 4
	//case shared.REJECTED:
	//return 5
	default:
		return 0
	}
}

// Convert bytes back to enums for deserialization
func byteToSide(b byte) shared.Side {
	switch b {
	case 1:
		return shared.BUY
	case 2:
		return shared.SELL
	default:
		return ""
	}
}

func byteToOrderType(b byte) shared.OrderType {
	switch b {
	case 1:
		return shared.LIMIT
	case 2:
		return shared.MARKET
	default:
		return ""
	}
}

func byteToTimeInForce(b byte) shared.TimeInForce {
	switch b {
	case 1:
		return shared.GTC
	case 2:
		return shared.IOC
	case 3:
		return shared.FOK
	default:
		return ""
	}
}

func byteToOrderStatus(b byte) shared.OrderStatus {
	switch b {
	case 1:
		return shared.PENDING
	case 2:
		return shared.PARTIALLY_FILLED
	case 3:
		return shared.COMPLETE
	case 4:
		return shared.CANCELLED
	//case 5:
	//return shared.REJECTED
	default:
		return ""
	}
}
