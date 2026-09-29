//go:build integration

package natsx

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"

// TestTracePropagatesToDLQ checks the trace ID survives publish -> consume ->
// max deliveries -> DLQ handler, for canonical, lowercase and missing headers.
func TestTracePropagatesToDLQ(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.Baggage{}, propagation.TraceContext{}))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr, err := NewEventManager()
	if err != nil {
		t.Skipf("nats not reachable: %v", err)
	}
	t.Cleanup(mgr.Close) // registered first so it runs after the stream deletes below

	name := fmt.Sprintf("natsxtest-%d", time.Now().UnixNano())
	subject := name + ".msg"
	stream, err := mgr.Js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = mgr.Js.DeleteStream(context.Background(), name)
		_ = mgr.Js.DeleteStream(context.Background(), name+"_dlq")
	})

	dlqGot := make(chan DLQMessage, 3)
	if err := mgr.StartDLQConsumer(ctx, name, func(_ context.Context, m DLQMessage) error {
		dlqGot <- m
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "c", FilterSubject: subject, AckPolicy: jetstream.AckExplicitPolicy,
		MaxDeliver: 2, AckWait: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumeGot := make(chan [2]string, 6)
	if err := mgr.Subscribe(consumer, func(hctx context.Context, m jetstream.Msg) error {
		consumeGot <- [2]string{string(m.Data()), trace.SpanContextFromContext(hctx).TraceID().String()}
		return errors.New("always fail")
	}); err != nil {
		t.Fatal(err)
	}

	tid, _ := trace.TraceIDFromHex(testTraceID)
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	pubCtx := trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))

	// canonical: written by Publish as "Traceparent".
	if err := mgr.Publish(pubCtx, subject, []byte("canonical")); err != nil {
		t.Fatal(err)
	}
	// lowercase: what the nats CLI and non-Go clients send.
	lower := nats.NewMsg(subject)
	lower.Data = []byte("lowercase")
	lower.Header["traceparent"] = []string{"00-" + testTraceID + "-00f067aa0ba902b7-01"}
	if _, err := mgr.Js.PublishMsg(ctx, lower); err != nil {
		t.Fatal(err)
	}
	// none: no trace context at all.
	if _, err := mgr.Js.Publish(ctx, subject, []byte("none")); err != nil {
		t.Fatal(err)
	}

	want := map[string]string{"canonical": testTraceID, "lowercase": testTraceID, "none": ""}
	for range want {
		select {
		case m := <-dlqGot:
			exp := want[string(m.Payload)]
			if m.TraceID != exp {
				t.Errorf("%s: dlq TraceID = %q, want %q", m.Payload, m.TraceID, exp)
			}
			if exp != "" && m.TraceParent == "" {
				t.Errorf("%s: dlq TraceParent empty", m.Payload)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for DLQ messages")
		}
	}

	for len(consumeGot) > 0 {
		c := <-consumeGot
		if want[c[0]] != "" && c[1] != testTraceID {
			t.Errorf("%s: consumer trace = %s, want %s", c[0], c[1], testTraceID)
		}
	}
}
