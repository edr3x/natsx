package natsx

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// ConsumerSpec defines the configuration for a JetStream consumer.
//
// The Name and Subject fields are required. All other fields have sensible defaults.
type ConsumerSpec struct {
	Name       string
	Subject    string
	MaxDeliver int
	AckWait    time.Duration // AckWait >= max(BackOff)
}

// Handler is invoked for each message delivered on a subject.
type Handler func(ctx context.Context, msg jetstream.Msg) error

// RegisterConsumer creates or updates a JetStream consumer based on the
// ConsumerSpec configuration.
//
// The method validates required fields (stream, name, subject) and applies
// sensible defaults when values are not provided:
//
//   - MaxDeliver defaults to 5
//   - AckPolicy is set to AckExplicitPolicy
//   - Durable name is derived from the consumer name
//   - A default exponential backoff is applied
//
// An optional jetstream.ConsumerConfig override may be provided. When supplied,
// the override fully replaces the generated configuration and is passed
// directly to JetStream without modification.
//
// This method is idempotent: calling it multiple times with the same
// configuration will not create duplicate consumers.
//
// Returns the created or updated JetStream consumer, or an error if validation
// or creation fails.
func (c *ConsumerSpec) RegisterConsumer(
	ctx context.Context,
	stream jetstream.Stream,
	configOverride ...jetstream.ConsumerConfig,
) (jetstream.Consumer, error) {
	// Validate required dependencies
	if stream == nil {
		return nil, errors.New("stream not provided")
	}

	// Validate required consumer fields
	if c.Name == "" {
		return nil, errors.New("name not provided")
	}

	if c.Subject == "" {
		return nil, errors.New("subject not provided")
	}

	// Apply default values
	if c.MaxDeliver == 0 {
		c.MaxDeliver = 5
	}

	if c.AckWait == 0 {
		c.AckWait = 1 * time.Minute
	}

	// Build default consumer configuration
	consumerConfig := jetstream.ConsumerConfig{
		Name:          c.Name,
		Durable:       c.Name,
		FilterSubject: c.Subject,
		MaxDeliver:    c.MaxDeliver,
		AckWait:       c.AckWait,
		AckPolicy:     jetstream.AckExplicitPolicy,
		BackOff: []time.Duration{
			5 * time.Second,
			10 * time.Second,
			15 * time.Second,
			20 * time.Second,
		},
	}

	// Apply override configuration if provided
	// NOTE: This replaces the entire configuration, not a merge.
	if len(configOverride) > 0 {
		consumerConfig = configOverride[0]
	}

	// Create or update the JetStream consumer
	return stream.CreateOrUpdateConsumer(ctx, consumerConfig)
}

// Subscribe starts a JetStream consumer and registers it for shutdown.
//
// Behavior:
//   - Messages are delivered asynchronously
//   - The consumer is stopped when ctx is cancelled
//   - The consumer is also stopped when Close() is called
//
// This method is safe to call multiple times.
func (m *Manager) Subscribe(consumer jetstream.Consumer, handler Handler) error {
	fn := func(msg jetstream.Msg) {
		// extract traceparent from header and set it
		carrier := propagation.MapCarrier{}
		if h := msg.Headers(); h != nil {
			carrier["traceparent"] = h.Get("traceparent")
		}

		ctx := otel.GetTextMapPropagator().Extract(
			context.Background(), // we do not pass the context from consumer as this needs to run on background always
			carrier,
		)

		ctx, span := otel.Tracer("nats.consumer").Start(
			ctx,
			"nats.consume "+msg.Subject(),
			trace.WithSpanKind(trace.SpanKindConsumer),
		)
		defer span.End()

		// delivery attempt + consumer metadata
		if meta, err := msg.Metadata(); err == nil {
			span.SetAttributes(
				attribute.Int64("nats.delivery_count", int64(meta.NumDelivered)),
				attribute.Int64("nats.stream_seq", int64(meta.Sequence.Stream)),
			)
		}

		span.SetAttributes(
			attribute.String("nats.subject", msg.Subject()),
			attribute.String("nats.stream", consumer.CachedInfo().Stream),
		)

		if err := handler(ctx, msg); err != nil {
			slog.Error("Erorr from nats subscriber handler", "subject", msg.Subject(), "error", err.Error())
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())

			// Check if this is a terminal error (should not retry)
			if IsTerminalError(err) {
				slog.Error("terminal error from nats handler, message will not be retried",
					"error", err.Error(),
					"subject", msg.Subject(),
				)
				// no need to redeliver as the error is fatal
				if termErr := msg.Term(); termErr != nil {
					slog.Error("failed to terminate message", "error", termErr)
					span.RecordError(termErr)
				}
				return
			}
			// Recoverable error - requeue for retry
			slog.Error("error from nats subscriber handler", "error", err.Error())
			return
		}

		// Successful, ack immediately
		if ackErr := msg.Ack(); ackErr != nil {
			slog.Error("failed to ack", "error", ackErr)
			span.RecordError(ackErr)
			span.SetStatus(codes.Error, ackErr.Error())
			return

		}

		span.SetStatus(codes.Ok, "processed")
	}

	cctx, err := consumer.Consume(fn, jetstream.PullMaxMessages(m.prefetch))
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.stopFuncs = append(m.stopFuncs, cctx.Stop)
	m.mu.Unlock()

	return nil
}
