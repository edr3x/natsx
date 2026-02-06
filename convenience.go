package natsx

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
)

var (
	defaultManager *Manager
	defaultMutex   sync.Mutex
	defaultErr     error
)

// DefaultManager returns the default Manager instance.
// The manager is created lazily on first call.
func DefaultManager() (*Manager, error) {
	defaultMutex.Lock()
	defer defaultMutex.Unlock()

	if defaultManager != nil {
		return defaultManager, nil
	}

	defaultManager, defaultErr = NewEventManager()
	return defaultManager, defaultErr
}

// SetDefaultManager sets the default manager instance.
// This is useful for testing or custom configuration.
func SetDefaultManager(m *Manager) {
	defaultManager = m
}

// SimpleHandler is a simplified handler that takes raw data instead of jetstream.Msg.
type SimpleHandler func(ctx context.Context, data []byte) error

// Subscribe is a convenience function that subscribes to a NATS stream.
// This is a simplified version for backward compatibility.
func Subscribe(ctx context.Context, stream, consumer string, handler SimpleHandler) error {
	mgr, err := DefaultManager()
	if err != nil {
		return err
	}

	// Get or create the stream's consumer
	s, err := mgr.Js.Stream(ctx, stream)
	if err != nil {
		return err
	}

	c, err := s.Consumer(ctx, consumer)
	if err != nil {
		return err
	}

	// Wrap the simple handler to match the events.Handler signature
	return mgr.Subscribe(c, func(ctx context.Context, msg jetstream.Msg) error {
		return handler(ctx, msg.Data())
	})
}

// Publish is a convenience function that uses the default manager.
func Publish(ctx context.Context, subject string, data []byte) error {
	mgr, err := DefaultManager()
	if err != nil {
		return err
	}
	return mgr.Publish(ctx, subject, data)
}
