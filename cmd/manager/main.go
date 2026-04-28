// Command manager is the entry point for the Sermo Profluens Manager service.
//
// Responsibilities:
//   - Consume NSQ room_events and persist RoomCreated records to MySQL.
//   - Watch etcd for room-worker registrations and log changes.
//
// Configuration is read from environment variables:
//
//	ETCD_ENDPOINTS   comma-separated etcd endpoints (default: localhost:2379)
//	NSQLOOKUPD_ADDR  nsqlookupd HTTP address       (default: 127.0.0.1:4161)
//	MYSQL_DSN        GORM MySQL DSN                (default: root:@tcp(127.0.0.1:3306)/sermo?parseTime=true)
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/Enuma3lish/sermo-profluens/internal/auditor"
	"github.com/Enuma3lish/sermo-profluens/internal/discovery"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── etcd ─────────────────────────────────────────────────────────────────
	etcdEndpoints := envOrDefault("ETCD_ENDPOINTS", "localhost:2379")
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(etcdEndpoints, ","),
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("manager: connect to etcd: %v", err)
	}
	defer etcdClient.Close()

	registry := discovery.New(etcdClient)

	// Watch room registrations in the background.
	go func() {
		updates := registry.WatchRooms(ctx)
		for u := range updates {
			log.Printf("manager: room update type=%s room=%s addr=%s",
				u.Type, u.Info.RoomID, u.Info.Address)
		}
	}()

	// ── MySQL + GORM ──────────────────────────────────────────────────────────
	dsn := envOrDefault("MYSQL_DSN", "root:@tcp(127.0.0.1:3306)/sermo?parseTime=true")
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("manager: open mysql: %v", err)
	}
	if err := db.AutoMigrate(&auditor.RoomRecord{}); err != nil {
		log.Fatalf("manager: auto-migrate: %v", err)
	}

	// ── NSQ consumer ─────────────────────────────────────────────────────────
	nsqlookupdAddr := envOrDefault("NSQLOOKUPD_ADDR", "127.0.0.1:4161")
	consumer, err := auditor.NewConsumer("room_events", "auditor", nsqlookupdAddr, db)
	if err != nil {
		log.Fatalf("manager: create auditor consumer: %v", err)
	}
	defer consumer.Stop()

	log.Println("manager: running – waiting for signals")
	<-ctx.Done()
	log.Println("manager: shutting down")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
