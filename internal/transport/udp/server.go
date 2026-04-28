// Package udp implements a high-performance UDP chat server for public rooms.
// Every incoming datagram is broadcast to all other registered participants.
// Participants that have been silent for 5 minutes are automatically evicted.
package udp

import (
	"log"
	"net"
	"sync"
	"time"
)

const (
	maxDatagramSize  = 65507
	inactiveTimeout  = 5 * time.Minute
	cleanupInterval  = 1 * time.Minute
)

// participant holds the address and the last time a message was received from
// that address.
type participant struct {
	addr     *net.UDPAddr
	lastSeen time.Time
}

// Server is a UDP broadcast server for a single public room.
type Server struct {
	conn *net.UDPConn

	mu           sync.RWMutex
	participants map[string]*participant // key is addr.String()
}

// NewServer creates a Server that listens on the supplied address
// (e.g. ":9000").
func NewServer(addr string) (*Server, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &Server{
		conn:         conn,
		participants: make(map[string]*participant),
	}, nil
}

// Serve reads datagrams in a loop and broadcasts each one to all other
// participants.  It returns when the underlying connection is closed.
func (s *Server) Serve() {
	go s.cleanupLoop()

	buf := make([]byte, maxDatagramSize)
	for {
		n, sender, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			// Connection closed – stop gracefully.
			log.Printf("udp: read error (server may be shutting down): %v", err)
			return
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		s.upsertParticipant(sender)
		s.broadcast(data, sender)
	}
}

// Close shuts down the UDP listener.
func (s *Server) Close() error {
	return s.conn.Close()
}

// upsertParticipant registers a new participant or refreshes their lastSeen
// timestamp.
func (s *Server) upsertParticipant(addr *net.UDPAddr) {
	key := addr.String()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.participants[key] = &participant{
		addr:     addr,
		lastSeen: time.Now(),
	}
}

// broadcast sends data to every participant except the original sender.
func (s *Server) broadcast(data []byte, sender *net.UDPAddr) {
	senderKey := sender.String()
	s.mu.RLock()
	defer s.mu.RUnlock()

	for key, p := range s.participants {
		if key == senderKey {
			continue
		}
		if _, err := s.conn.WriteToUDP(data, p.addr); err != nil {
			log.Printf("udp: send to %s: %v", key, err)
		}
	}
}

// cleanupLoop periodically removes participants that have been inactive for
// longer than inactiveTimeout.
func (s *Server) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.evictInactive()
	}
}

// evictInactive removes stale participants.
func (s *Server) evictInactive() {
	cutoff := time.Now().Add(-inactiveTimeout)
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, p := range s.participants {
		if p.lastSeen.Before(cutoff) {
			log.Printf("udp: evicting inactive participant %s (last seen %s ago)",
				key, time.Since(p.lastSeen).Round(time.Second))
			delete(s.participants, key)
		}
	}
}

// ParticipantCount returns the number of currently tracked participants.
func (s *Server) ParticipantCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.participants)
}
