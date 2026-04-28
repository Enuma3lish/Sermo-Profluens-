// Command room-worker is the entry point for a Sermo Profluens room-worker
// instance.  A single binary can serve either a private (WebSocket) or a
// public (UDP) room depending on the ROOM_TYPE environment variable.
//
// Configuration is read from environment variables:
//
//	ROOM_ID         unique room identifier   (required)
//	ROOM_TYPE       "private" or "public"    (default: private)
//	LISTEN_ADDR     bind address             (default: :8080 for ws, :9000 for udp)
//	ADVERTISE_ADDR  address registered in etcd (default: same as LISTEN_ADDR)
//	ETCD_ENDPOINTS  comma-separated etcd endpoints (default: localhost:2379)
//	REDIS_ADDR      Redis address for ws rooms     (default: localhost:6379)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/redis/go-redis/v9"

	"github.com/Enuma3lish/sermo-profluens/internal/discovery"
	"github.com/Enuma3lish/sermo-profluens/internal/transport/udp"
	"github.com/Enuma3lish/sermo-profluens/internal/transport/ws"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	roomID := mustEnv("ROOM_ID")
	roomTypeStr := envOrDefault("ROOM_TYPE", "private")
	roomType := discovery.RoomType(roomTypeStr)

	// ── etcd registration ─────────────────────────────────────────────────────
	etcdEndpoints := envOrDefault("ETCD_ENDPOINTS", "localhost:2379")
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("room-worker: connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	registry := discovery.New(etcdClient)

	// Start the appropriate server and collect the listen address.
	listenAddr := envOrDefault("LISTEN_ADDR", defaultListenAddr(roomType))
	advertiseAddr := envOrDefault("ADVERTISE_ADDR", listenAddr)

	if err := registry.RegisterRoom(ctx, roomID, advertiseAddr, roomType); err != nil {
		log.Fatalf("room-worker: register room: %v", err)
	}
	go func() {
		if err := registry.KeepAlive(ctx); err != nil {
			log.Printf("room-worker: keepalive: %v", err)
		}
	}()
	defer func() {
		revokeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := registry.Revoke(revokeCtx); err != nil {
			log.Printf("room-worker: revoke lease: %v", err)
		}
	}()

	switch roomType {
	case discovery.RoomTypePrivate:
		serveWebSocket(ctx, roomID, listenAddr)
	case discovery.RoomTypePublic:
		serveUDP(ctx, listenAddr)
	default:
		log.Fatalf("room-worker: unknown room type %q", roomType)
	}
}

// serveWebSocket starts the HTTP/WebSocket server for a private room.
func serveWebSocket(ctx context.Context, roomID, addr string) {
	redisAddr := envOrDefault("REDIS_ADDR", "localhost:6379")
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})

	handler := ws.NewHandler(roomID, rdb)
	mux := http.NewServeMux()
	mux.Handle("/ws", handler)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("room-worker: ws shutdown: %v", err)
		}
	}()

	log.Printf("room-worker: WebSocket server listening on %s (room=%s)", addr, roomID)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("room-worker: ws listen: %v", err)
	}
}

// serveUDP starts the UDP broadcast server for a public room.
func serveUDP(ctx context.Context, addr string) {
	srv, err := udp.NewServer(addr)
	if err != nil {
		log.Fatalf("room-worker: udp listen: %v", err)
	}

	go func() {
		<-ctx.Done()
		if err := srv.Close(); err != nil {
			log.Printf("room-worker: udp close: %v", err)
		}
	}()

	log.Printf("room-worker: UDP server listening on %s", addr)
	srv.Serve()
}

func defaultListenAddr(rt discovery.RoomType) string {
	if rt == discovery.RoomTypePublic {
		return ":9000"
	}
	return ":8080"
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("room-worker: required environment variable %s is not set", key)
	}
	return v
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
