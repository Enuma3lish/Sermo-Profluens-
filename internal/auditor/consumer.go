// Package auditor implements an NSQ consumer that listens for room_events
// messages and persists RoomCreated events to MySQL using GORM.
package auditor

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
	"gorm.io/gorm"
)

// RoomCreatedEvent is the JSON payload published to the room_events NSQ topic.
type RoomCreatedEvent struct {
	EventType string    `json:"event_type"`
	RoomID    string    `json:"room_id"`
	Creator   string    `json:"creator"`
	Topic     string    `json:"topic"`
	Timestamp time.Time `json:"timestamp"`
	RoomType  string    `json:"room_type"`
}

// RoomRecord is the GORM model persisted to MySQL.
type RoomRecord struct {
	ID        uint      `gorm:"primaryKey;autoIncrement"`
	RoomID    string    `gorm:"uniqueIndex;not null"`
	Creator   string    `gorm:"not null"`
	Topic     string
	RoomType  string    `gorm:"not null"`
	CreatedAt time.Time `gorm:"not null"`
}

// Consumer subscribes to the room_events NSQ topic and persists records to
// MySQL via GORM.
type Consumer struct {
	consumer *nsq.Consumer
	db       *gorm.DB
}

// NewConsumer creates a new Consumer.
// topic is the NSQ topic name (e.g. "room_events").
// channel is the NSQ channel name (e.g. "auditor").
// nsqlookupdAddr is the nsqlookupd HTTP address (e.g. "127.0.0.1:4161").
func NewConsumer(topic, channel, nsqlookupdAddr string, db *gorm.DB) (*Consumer, error) {
	cfg := nsq.NewConfig()
	c, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		return nil, fmt.Errorf("auditor: create nsq consumer: %w", err)
	}

	a := &Consumer{consumer: c, db: db}
	c.AddHandler(a)

	if err := c.ConnectToNSQLookupd(nsqlookupdAddr); err != nil {
		return nil, fmt.Errorf("auditor: connect to nsqlookupd %s: %w", nsqlookupdAddr, err)
	}

	return a, nil
}

// HandleMessage implements nsq.Handler.  It processes room_events messages and
// persists RoomCreated events to MySQL inside a transaction.
func (a *Consumer) HandleMessage(msg *nsq.Message) error {
	var event RoomCreatedEvent
	if err := json.Unmarshal(msg.Body, &event); err != nil {
		log.Printf("auditor: unmarshal message: %v", err)
		// Returning nil so NSQ does not re-queue malformed messages.
		return nil
	}

	if event.EventType != "RoomCreated" {
		// Not a RoomCreated event; nothing to do.
		return nil
	}

	record := RoomRecord{
		RoomID:    event.RoomID,
		Creator:   event.Creator,
		Topic:     event.Topic,
		RoomType:  event.RoomType,
		CreatedAt: event.Timestamp,
	}

	if err := a.persistWithTx(record); err != nil {
		log.Printf("auditor: persist room %s: %v", event.RoomID, err)
		// Return the error so NSQ requeues the message for retry.
		return err
	}

	log.Printf("auditor: persisted room %s (creator=%s)", event.RoomID, event.Creator)
	return nil
}

// persistWithTx wraps the GORM insert in an explicit database transaction.
func (a *Consumer) persistWithTx(record RoomRecord) error {
	return a.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&record).Error; err != nil {
			return fmt.Errorf("insert room_record: %w", err)
		}
		return nil
	})
}

// Stop gracefully shuts down the NSQ consumer.
func (a *Consumer) Stop() {
	a.consumer.Stop()
	<-a.consumer.StopChan
}
