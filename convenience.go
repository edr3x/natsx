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
//
// The manager is created lazily on first call using default options.
// Subsequent calls return the same instance.
//
// This is a convenience function for applications that only need
// a single NATS connection. For more control, use NewEventManager directly.
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
//
// This is useful for testing or when you need to use a custom-configured
// manager with the convenience functions like Publish and Subscribe.
//
// Note: This function is not safe for concurrent use with DefaultManager()
// in other goroutines.
func SetDefaultManager(m *Manager) {
	defaultManager = m
}

// SimpleHandler is a simplified handler that takes raw data instead of jetstream.Msg.
type SimpleHandler func(ctx context.Context, data []byte) error

// Subscribe is a convenience function that subscribes to a NATS stream
// using the default manager.
//
// It retrieves the stream and consumer from JetStream and registers
// a handler to process messages. The handler receives the raw message
// data as []byte rather than a jetstream.Msg.
//
// This is a simplified version for common use cases. For more control,
// use Manager.Subscribe directly.
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

// Publish is a convenience function that publishes a message
// using the default manager.
//
// This is a simplified version for common use cases. For more control,
// use Manager.Publish directly.
func Publish(ctx context.Context, subject string, data []byte) error {
	mgr, err := DefaultManager()
	if err != nil {
		return err
	}
	return mgr.Publish(ctx, subject, data)
}
