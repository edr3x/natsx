// Package natsx provides a NATS + JetStream client manager.
//
// It is intended to be used by both publishers and consumers.
//
// Responsibilities:
//   - Establish and own a NATS connection
//   - Provide a JetStream context
//   - Manage lifecycle of JetStream consumers (optional)
//   - Gracefully shut down consumers and connections
//
// This package does NOT:
//   - Create streams
//   - Define subjects or schemas
//   - Handle retries (DLQ forwarding is optional)
//
// Lifecycle:
//   - A Manager is created by the application (usually in main())
//   - The caller owns the Manager and MUST call Close()
//
// Usage:
//
//	mgr, err := natsx.New()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer mgr.Close()
//
//	// Publishing
//	mgr.Js.Publish(ctx, "orders.created", data)
//
//	// Consuming
//	mgr.StartConsumer(ctx, consumer, handler)
package natsx

import (
	"cmp"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Manager is the central NATS/JetStream client.
type Manager struct {
	Nc *nats.Conn
	Js jetstream.JetStream

	// Config
	natsURL  string
	prefetch int // The pull max messages count

	// Auth
	username string
	password string

	mu        sync.Mutex
	stopFuncs []func()
}

// Option defines a functional configuration for the Manager.
type Option func(*Manager)

// WithURL sets the NATS connection URL.
func WithURL(url string) Option {
	return func(m *Manager) {
		m.natsURL = url
	}
}

// WithConsumerPrefetch sets the maximum number of messages to buffer per consumer.
// Set this to 1 if your processing is slow or synchronous.
func WithConsumerPrefetch(count int) Option {
	return func(m *Manager) {
		m.prefetch = count
	}
}

// New creates and returns a new Manager.
//
// This is a convenience alias for NewEventManager.
// Multiple calls return the same instance.
// This function is safe for concurrent use.
//
// The caller MUST call Close() during application shutdown.
//
// Example:
//
//	mgr, err := natsx.New()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer mgr.Close()
func New(opts ...Option) (*Manager, error) {
	return NewEventManager(opts...)
}

// NewEventManager creates and returns a new Manager.
//
// Multiple calls return the same instance.
// This function is safe for concurrent use.
//
// The caller MUST call Close() during application shutdown.
func NewEventManager(opts ...Option) (*Manager, error) {
	m := &Manager{
		natsURL: cmp.Or(os.Getenv("NATS_URL"), nats.DefaultURL),

		// This ensures only 1 message is in flight at a time.
		// The next message won't be pulled until the current one is handled.
		prefetch: 1,
	}

	for _, opt := range opts {
		opt(m)
	}

	nc, e := nats.Connect(m.natsURL,
		nats.ReconnectWait(2*time.Second),
		nats.MaxReconnects(10),
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			if err != nil {
				slog.Error("nats error", "error", err, "subject", subjectName(sub))
			}
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				slog.Warn("nats disconnected", "error", err)
			}
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			slog.Info("nats reconnected", "server", conn.ConnectedUrl())
		}),
		nats.ClosedHandler(func(conn *nats.Conn) {
			if err := conn.LastError(); err != nil {
				slog.Warn("nats connection closed", "error", err)
			}
		}),
	)
	if e != nil {
		return nil, e
	}

	js, e := jetstream.New(nc)
	if e != nil {
		nc.Close()
		return nil, e
	}

	m.Nc = nc
	m.Js = js

	return m, nil
}

func subjectName(sub *nats.Subscription) string {
	if sub == nil {
		return ""
	}
	return sub.Subject
}

// Close gracefully stops all consumers and closes the NATS connection.
//
// This should be called exactly once during application shutdown.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, stop := range m.stopFuncs {
		stop()
	}
	m.stopFuncs = nil

	if m.Nc != nil {
		_ = m.Nc.Drain()
		m.Nc.Close()
	}
}
