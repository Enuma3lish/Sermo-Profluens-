// Package discovery provides etcd-backed service registration and discovery
// for Sermo Profluens room workers.
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// leaseTTL is the etcd lease time-to-live in seconds.
	leaseTTL = 10
	// roomKeyPrefix is the etcd key namespace for room registrations.
	roomKeyPrefix = "/services/rooms/"
)

// RoomType classifies a room as private (WebSocket) or public (UDP).
type RoomType string

const (
	RoomTypePrivate RoomType = "private"
	RoomTypePublic  RoomType = "public"
)

// RoomInfo holds the metadata stored in etcd for a single room worker.
type RoomInfo struct {
	RoomID   string   `json:"room_id"`
	Address  string   `json:"address"`
	RoomType RoomType `json:"room_type"`
}

// Registry manages etcd-backed service registration and discovery.
type Registry struct {
	client  *clientv3.Client
	leaseID clientv3.LeaseID
}

// New creates a Registry backed by the provided etcd client.
func New(client *clientv3.Client) *Registry {
	return &Registry{client: client}
}

// roomKey returns the etcd key for the given room.
func roomKey(roomType RoomType, roomID string) string {
	return fmt.Sprintf("%s%s/%s", roomKeyPrefix, string(roomType), roomID)
}

// RegisterRoom puts a key into etcd under /services/rooms/<type>/<id> with a
// 10-second TTL lease.  Call KeepAlive in a separate goroutine afterwards.
func (r *Registry) RegisterRoom(ctx context.Context, roomID, addr string, roomType RoomType) error {
	lease, err := r.client.Grant(ctx, leaseTTL)
	if err != nil {
		return fmt.Errorf("discovery: grant lease: %w", err)
	}
	r.leaseID = lease.ID

	info := RoomInfo{
		RoomID:   roomID,
		Address:  addr,
		RoomType: roomType,
	}
	value, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("discovery: marshal room info: %w", err)
	}

	key := roomKey(roomType, roomID)
	_, err = r.client.Put(ctx, key, string(value), clientv3.WithLease(r.leaseID))
	if err != nil {
		return fmt.Errorf("discovery: put key %s: %w", key, err)
	}

	log.Printf("discovery: registered room %s at %s (lease %x)", roomID, addr, r.leaseID)
	return nil
}

// KeepAlive periodically renews the lease until ctx is cancelled.
// It should be started in a dedicated goroutine right after RegisterRoom.
func (r *Registry) KeepAlive(ctx context.Context) error {
	keepAliveCh, err := r.client.KeepAlive(ctx, r.leaseID)
	if err != nil {
		return fmt.Errorf("discovery: start keepalive: %w", err)
	}

	for {
		select {
		case resp, ok := <-keepAliveCh:
			if !ok {
				return fmt.Errorf("discovery: keepalive channel closed (lease %x)", r.leaseID)
			}
			log.Printf("discovery: lease %x renewed (ttl=%d)", resp.ID, resp.TTL)
		case <-ctx.Done():
			log.Printf("discovery: keepalive stopped: %v", ctx.Err())
			return nil
		}
	}
}

// RoomUpdate represents a single etcd watch event for a room key.
type RoomUpdate struct {
	Type string   // "PUT" or "DELETE"
	Info RoomInfo // populated for PUT events
}

// WatchRooms watches all keys under /services/rooms/ and streams updates via
// the returned channel.  The channel is closed when ctx is cancelled.
func (r *Registry) WatchRooms(ctx context.Context) <-chan RoomUpdate {
	ch := make(chan RoomUpdate, 64)

	watchCh := r.client.Watch(ctx, roomKeyPrefix, clientv3.WithPrefix())
	go func() {
		defer close(ch)
		for {
			select {
			case resp, ok := <-watchCh:
				if !ok {
					return
				}
				for _, ev := range resp.Events {
					update := RoomUpdate{}
					switch ev.Type {
					case clientv3.EventTypePut:
						update.Type = "PUT"
						if err := json.Unmarshal(ev.Kv.Value, &update.Info); err != nil {
							log.Printf("discovery: unmarshal event value: %v", err)
							continue
						}
					case clientv3.EventTypeDelete:
						update.Type = "DELETE"
					}
					select {
					case ch <- update:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

// Revoke explicitly revokes the current lease, causing the room key to be
// immediately removed from etcd.
func (r *Registry) Revoke(ctx context.Context) error {
	if r.leaseID == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.client.Revoke(ctx, r.leaseID)
	return err
}
