// Package ws implements a WebSocket chat handler backed by Redis Pub/Sub for
// cross-node message broadcasting.
package ws

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Allow all origins for this example; tighten as required.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Handler handles WebSocket connections for a single private room.
// It uses Redis Pub/Sub so that messages are broadcast to every node serving
// the same room.
type Handler struct {
	roomID string
	rdb    *redis.Client

	// conns tracks active *websocket.Conn on this node.
	conns sync.Map
}

// NewHandler creates a Handler for the given room backed by the supplied
// Redis client.
func NewHandler(roomID string, rdb *redis.Client) *Handler {
	return &Handler{roomID: roomID, rdb: rdb}
}

// ServeHTTP upgrades the HTTP connection and starts the read/write loops.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws: upgrade error: %v", err)
		return
	}

	h.conns.Store(conn, struct{}{})
	defer func() {
		h.conns.Delete(conn)
		conn.Close()
	}()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	send := make(chan []byte, 256)

	// Subscribe to the Redis channel for this room.
	sub := h.rdb.Subscribe(ctx, h.roomID)
	defer sub.Close()

	// goroutine 1: receive messages from Redis and forward to the WebSocket.
	go func() {
		for {
			msg, err := sub.ReceiveMessage(ctx)
			if err != nil {
				cancel()
				return
			}
			select {
			case send <- []byte(msg.Payload):
			case <-ctx.Done():
				return
			}
		}
	}()

	// goroutine 2: write messages from the send channel to the WebSocket.
	go h.writePump(ctx, conn, send, cancel)

	// Read pump (runs in the calling goroutine).
	h.readPump(ctx, conn, cancel)
}

// readPump reads messages from the WebSocket client and publishes them to the
// Redis Pub/Sub channel so all nodes (and all subscribers) receive them.
func (h *Handler) readPump(ctx context.Context, conn *websocket.Conn, cancel context.CancelFunc) {
	defer cancel()

	conn.SetReadLimit(maxMessageSize)
	_ = conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("ws: read error: %v", err)
			}
			return
		}

		if err := h.rdb.Publish(ctx, h.roomID, message).Err(); err != nil {
			log.Printf("ws: redis publish error: %v", err)
		}
	}
}

// writePump drains the send channel and writes messages to the WebSocket,
// also sending periodic pings to keep the connection alive.
func (h *Handler) writePump(ctx context.Context, conn *websocket.Conn, send <-chan []byte, cancel context.CancelFunc) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		cancel()
	}()

	for {
		select {
		case message, ok := <-send:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
				log.Printf("ws: write error: %v", err)
				return
			}

		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

// ActiveConnections returns the number of WebSocket connections currently
// tracked on this node for the room.
func (h *Handler) ActiveConnections() int {
	count := 0
	h.conns.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
