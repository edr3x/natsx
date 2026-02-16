package natsx

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// DLQMessage represents a message that has exceeded its maximum delivery
// attempts and has been moved to the dead letter queue.
//
// It contains the original message payload, headers, and metadata about
// the delivery failure.
type DLQMessage struct {
	Advisory MaxDeliveriesAdvisory

	// Original message data
	Subject string
	Headers nats.Header
	Payload []byte

	// Observability
	TraceParent string
	TraceID     string

	// Convenience
	ReceivedAt time.Time
}

// DLQHandler is a function that processes messages from the dead letter queue.
//
// The handler receives the original message that failed to be processed
// after all retry attempts. It should handle the failure gracefully, such as
// logging, storing in a separate system, or alerting.
type DLQHandler func(ctx context.Context, msg DLQMessage) error

// MaxDeliveriesAdvisory is a JetStream advisory that is emitted when a message
// exceeds its maximum delivery attempts.
//
// This advisory is used by the DLQ consumer to retrieve and process the
// failed message.
type MaxDeliveriesAdvisory struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	Timestamp string `json:"timestamp"`

	Stream   string `json:"stream"`
	Consumer string `json:"consumer"`

	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
}

// StartDLQConsumer creates a dead letter queue consumer for the given stream.
//
// This method:
//
//  1. Creates a DLQ stream named {originalStream}_dlq
//  2. Creates a consumer on the DLQ stream
//  3. Starts consuming max delivery advisories
//  4. Retrieves the original failed message and passes it to the handler
//
// The DLQ consumer is automatically stopped when Close() is called on the Manager.
//
// Parameters:
//   - originalStream: The name of the original JetStream stream to monitor
//   - handler: The function to call for each failed message
func (m *Manager) StartDLQConsumer(
	originalStream string,
	handler DLQHandler,
) error {
	// background context to make consumer listen indefinitely
	ctx := context.Background()

	dlqStreamName := originalStream + "_dlq"

	// 1. Create / update DLQ stream
	stream, err := m.Js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        dlqStreamName,
		Description: "Dead letter queue for " + originalStream,
		Discard:     jetstream.DiscardNew,
		Metadata: map[string]string{
			"dead_letter_queue": "true",
			"source_stream":     originalStream,
		},
		Subjects: []string{
			"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + originalStream + ".>",
		},
	})
	if err != nil {
		return err
	}

	consumerConfig := jetstream.ConsumerConfig{
		Name:      dlqStreamName,
		Durable:   dlqStreamName,
		AckPolicy: jetstream.AckExplicitPolicy,
	}

	// 2. Create DLQ consumer
	consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig)
	if err != nil {
		return err
	}

	// 3. Start consuming advisories
	fn := func(msg jetstream.Msg) {
		var advisory MaxDeliveriesAdvisory
		if err := json.Unmarshal(msg.Data(), &advisory); err != nil {
			log.Printf("[DLQ ERROR] Failed to unmarshal advisory: %v", err)
			// ack & drop
			msg.Ack()
			return
		}

		origStream, err := m.Js.Stream(ctx, advisory.Stream)
		if err != nil {
			log.Printf("[DLQ ERROR] Failed to get stream handle: %v", err)
			return // retry
		}

		origMsg, err := origStream.GetMsg(ctx, advisory.StreamSeq)
		if err != nil {
			if err == nats.ErrMsgNotFound || err == jetstream.ErrMsgNotFound {
				log.Printf("[DLQ INFO] Message seq %d was already deleted from stream %s. Cannot archive payload.",
					advisory.StreamSeq, advisory.Stream)
				msg.Ack()
				return // return so it gets ACKnowledged and stops the loop
			}
			return // retry on other error
		}

		traceParent := ""
		if h := origMsg.Header; h != nil {
			traceParent = h.Get("traceparent")
		}

		// Rehydrate context
		ctx := otel.GetTextMapPropagator().Extract(
			ctx,
			propagation.MapCarrier{
				"traceparent": traceParent,
			},
		)

		ctx, span := otel.Tracer("nats.dlq").Start(
			ctx,
			"dlq.process."+origMsg.Subject,
			trace.WithSpanKind(trace.SpanKindConsumer),
		)
		defer span.End()

		span.SetAttributes(
			attribute.String("dlq.stream", advisory.Stream),
			attribute.String("dlq.consumer", advisory.Consumer),
			attribute.Int64("dlq.deliveries", int64(advisory.Deliveries)),
			attribute.Int64("dlq.stream_seq", int64(advisory.StreamSeq)),
		)

		// Build DLQ envelope
		dlqMsg := DLQMessage{
			Advisory:    advisory,
			Subject:     origMsg.Subject,
			Headers:     origMsg.Header,
			Payload:     origMsg.Data,
			TraceParent: traceParent,
			TraceID:     trace.SpanContextFromContext(ctx).TraceID().String(),
			ReceivedAt:  time.Now(),
		}

		// NOTE: DLQ handlers must never cause advisory retries.
		// Advisory is always ACKed to avoid DLQ retry loops.
		if err := handler(ctx, dlqMsg); err != nil {
			log.Printf("[DLQ Handler ERROR] %v", err)
			span.SetAttributes(attribute.String("error.dlq.handler", err.Error()))
			// Even if the handler fails, we usually ACK the advisory
			// to prevent an infinite loop of death for a single bad message.
		}

		if err := msg.Ack(); err != nil {
			log.Printf("[DLQ ERROR] Failed to ACK advisory: %v", err)
		}
	}

	cctx, err := consumer.Consume(fn)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.stopFuncs = append(m.stopFuncs, cctx.Stop)
	m.mu.Unlock()

	return nil
}
