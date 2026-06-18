package natsx

import (
	"context"
	"maps"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Publish publishes a message to the specified NATS subject with OpenTelemetry tracing.
//
// The function automatically injects trace context into the message headers
// for distributed tracing across services.
//
// Parameters:
//   - ctx: Context with trace information
//   - subject: NATS subject to publish to
//   - data: Message payload
//
// Returns any error encountered during publishing.
func (m *Manager) Publish(
	ctx context.Context,
	subject string,
	data []byte,
) error {
	ctx, span := otel.Tracer("nats.publisher").Start(
		ctx,
		"nats.publish "+subject,
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()

	// Inject trace context into message headers for distributed tracing
	headers := nats.Header{}

	propagator := otel.GetTextMapPropagator()
	propagator.Inject(ctx, propagation.HeaderCarrier(headers))

	// Create message with trace context headers
	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  headers,
	}

	// Publish message via JetStream
	_, err := m.Js.PublishMsg(ctx, msg)
	return err
}

// PublishWithHeaders publishes a message with caller-supplied extra headers merged
// alongside the injected OTel trace context. Use this for stamping correlation
// headers (e.g. Dlq-Replay-Of) on replayed messages.
func (m *Manager) PublishWithHeaders(
	ctx context.Context,
	subject string,
	data []byte,
	extraHeaders map[string][]string,
) error {
	ctx, span := otel.Tracer("nats.publisher").Start(
		ctx,
		"nats.publish "+subject,
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()

	headers := nats.Header{}
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(headers))
	maps.Copy(headers, extraHeaders)

	msg := &nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  headers,
	}

	_, err := m.Js.PublishMsg(ctx, msg)
	return err
}
