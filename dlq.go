package natsx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tunables. Exposed as package vars so callers can override before startup
// without us needing a full options struct yet. Promote to a Config struct
// once you need more knobs.
var (
	// DLQStreamMaxAge bounds how long failed-message records sit in JetStream
	// before being aged out. Your long-term store (Postgres, etc.) is the
	// source of truth; the stream is just a durable buffer.
	DLQStreamMaxAge = 30 * 24 * time.Hour

	// DLQStreamMaxBytes caps total bytes for the DLQ stream. Combined with
	// DiscardNew, this means an overflowing DLQ rejects new advisories rather
	// than evicting old ones — so you notice the problem instead of silently
	// losing failures.
	DLQStreamMaxBytes int64 = 1 << 30 // 1 GiB

	// DLQConsumerMaxDeliver caps retries for a single advisory. If we can't
	// process an advisory after this many tries (e.g. source stream is gone
	// permanently), we let JetStream stop redelivering rather than spin forever.
	DLQConsumerMaxDeliver = 5

	// DLQConsumerAckWait is how long JetStream waits for us to ack before
	// considering the advisory un-acked and redelivering.
	DLQConsumerAckWait = 30 * time.Second

	// DLQFetchTimeout bounds per-message NATS operations (Stream lookup,
	// GetMsg). Without this, a slow/unreachable server can wedge the consumer
	// goroutine indefinitely.
	DLQFetchTimeout = 5 * time.Second
)

// DLQMessage is the envelope passed to user-provided handlers. It contains
// the advisory metadata plus the rehydrated original message — or, if the
// original payload has already aged out of its source stream, the advisory
// alone with PayloadLost = true.
type DLQMessage struct {
	Advisory MaxDeliveriesAdvisory

	// Original message data. May be zero-valued if PayloadLost is true.
	Subject string
	Headers nats.Header
	Payload []byte

	// PayloadLost indicates the original message was no longer available in
	// the source stream when we tried to fetch it (usually due to retention).
	// Handlers should still persist the advisory for audit purposes.
	PayloadLost bool

	// Observability
	TraceParent string
	TraceID     string

	// ReceivedAt is when *we* observed the advisory, not when the original
	// message failed. Use Advisory.Timestamp for the failure time.
	ReceivedAt time.Time
}

// DLQHandler is the user-provided callback for processing dead-letter messages.
// Returning an error is logged and recorded on the span, but does NOT cause
// redelivery — we always ack advisories to prevent infinite loops on a single
// bad message. Handlers must be idempotent because JetStream may redeliver
// advisories on transient infrastructure failures (network blips, restarts).
type DLQHandler func(ctx context.Context, msg DLQMessage) error

// MaxDeliveriesAdvisory mirrors the JSON shape JetStream publishes on
// $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer> when a
// message exceeds its consumer's MaxDeliver setting.
type MaxDeliveriesAdvisory struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Timestamp  string `json:"timestamp"` // RFC3339; parse with time.Parse if needed
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries uint64 `json:"deliveries"`
}

// StartDLQConsumer wires up a dead-letter queue for a single source stream.
//
// It creates (or updates) a dedicated DLQ stream that captures MAX_DELIVERIES
// advisories for the source stream, then starts a durable consumer that
// rehydrates the original message and invokes `handler` for each failure.
//
// The returned consumer runs until `ctx` is cancelled or `Manager.Stop` is
// called. In-flight handler invocations receive the same `ctx` and should
// honor cancellation for clean shutdown.
//
// Idempotency note: handlers may be invoked more than once for the same
// advisory under transient failure conditions. Use Advisory.ID or
// (Advisory.Stream, Advisory.StreamSeq) as a dedup key in your store.
func (m *Manager) StartDLQConsumer(
	ctx context.Context,
	originalStream string,
	handler DLQHandler,
) error {
	dlqStreamName := originalStream + "_dlq"
	logger := slog.Default().With(
		slog.String("component", "natsx.dlq"),
		slog.String("source_stream", originalStream),
		slog.String("dlq_stream", dlqStreamName),
	)

	// 1. Create / update the DLQ stream.
	//
	// We capture advisories only for the configured source stream — note that
	// JetStream allows exactly one stream to own a given subject, so calling
	// this twice for the same originalStream is fine (CreateOrUpdate), but two
	// different processes claiming the same source will conflict.
	//
	// DiscardNew + MaxBytes/MaxAge: when the DLQ fills up, we reject new
	// advisories rather than evicting old ones. This makes overflow visible
	// (you'll see publish errors in NATS logs) instead of silently losing
	// failure records.
	stream, err := m.Js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        dlqStreamName,
		Description: "Dead letter queue for " + originalStream,
		Subjects: []string{
			"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + originalStream + ".>",
		},
		Storage:  jetstream.FileStorage,
		Discard:  jetstream.DiscardNew,
		MaxAge:   DLQStreamMaxAge,
		MaxBytes: DLQStreamMaxBytes,
		Metadata: map[string]string{
			"dead_letter_queue": "true",
			"source_stream":     originalStream,
		},
	})
	if err != nil {
		return fmt.Errorf("create dlq stream %q: %w", dlqStreamName, err)
	}

	// 2. Create / update the DLQ consumer.
	//
	// MaxDeliver caps retries: if processing an advisory fails repeatedly
	// (e.g. the source stream was deleted), we eventually give up rather than
	// retry forever. The consumer name is suffixed so we can later add other
	// consumers (archive, alerting) on the same DLQ stream without collision.
	consumerName := dlqStreamName + "_handler"
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:        consumerName,
		Durable:     consumerName,
		AckPolicy:   jetstream.AckExplicitPolicy,
		AckWait:     DLQConsumerAckWait,
		MaxDeliver:  DLQConsumerMaxDeliver,
		Description: "DLQ handler consumer for " + originalStream,
	})
	if err != nil {
		return fmt.Errorf("create dlq consumer %q: %w", consumerName, err)
	}

	// 3. Start consuming advisories.
	fn := func(msg jetstream.Msg) {
		// Per-message context derived from the consumer-lifetime ctx, so
		// shutdown cancels in-flight work and slow NATS calls can't wedge us.
		msgCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var advisory MaxDeliveriesAdvisory
		if err := json.Unmarshal(msg.Data(), &advisory); err != nil {
			// A malformed advisory is unrecoverable — log loudly and ack so
			// we don't redeliver garbage forever.
			logger.Error("failed to unmarshal advisory; dropping",
				slog.Any("err", err),
				slog.String("data", string(msg.Data())),
			)
			_ = msg.Ack()
			return
		}

		mlog := logger.With(
			slog.String("advisory_id", advisory.ID),
			slog.String("advisory_stream", advisory.Stream),
			slog.String("advisory_consumer", advisory.Consumer),
			slog.Uint64("stream_seq", advisory.StreamSeq),
			slog.Uint64("deliveries", advisory.Deliveries),
		)

		// Attempt to rehydrate the original message. We give each NATS call
		// its own timeout so a hung server doesn't block consumer progress.
		dlqMsg, payloadFetched, retry := m.fetchOriginal(msgCtx, advisory, mlog)
		if retry {
			// fetchOriginal decided this is a transient error worth retrying.
			// Return without acking — JetStream will redeliver up to
			// MaxDeliver times before giving up.
			return
		}

		// Trace context propagation. We extract from the original message's
		// headers so the DLQ span chains onto the producer's
		// trace, making cross-service debugging tractable.
		spanCtx := otel.GetTextMapPropagator().Extract(
			msgCtx,
			headerCarrier(dlqMsg.Headers),
		)
		// Only record a trace ID we actually inherited; otherwise the DLQ span is
		// a fresh root (or all zeros under a no-op tracer) and would mislead lookups.
		if parent := trace.SpanContextFromContext(spanCtx); parent.IsValid() {
			dlqMsg.TraceID = parent.TraceID().String()
		}
		spanCtx, span := otel.Tracer("nats.dlq").Start(
			spanCtx,
			"dlq.process."+advisory.Stream,
			trace.WithSpanKind(trace.SpanKindConsumer),
		)
		defer span.End()

		span.SetAttributes(
			attribute.String("dlq.stream", advisory.Stream),
			attribute.String("dlq.consumer", advisory.Consumer),
			attribute.Int64("dlq.deliveries", int64(advisory.Deliveries)),
			attribute.Int64("dlq.stream_seq", int64(advisory.StreamSeq)),
			attribute.Bool("dlq.payload_lost", !payloadFetched),
		)

		// Invoke the user handler with panic recovery. A panic here would
		// otherwise tear down the consumer goroutine silently and stop all
		// further DLQ processing for this stream.
		func() {
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("handler panic: %v", r)
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					mlog.Error("dlq handler panicked",
						slog.Any("panic", r),
						slog.String("trace_id", dlqMsg.TraceID),
					)
				}
			}()
			if err := handler(spanCtx, dlqMsg); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				mlog.Error("dlq handler returned error",
					slog.Any("err", err),
					slog.String("trace_id", dlqMsg.TraceID),
				)
				// Note: we still ack below. Advisory retries cause more
				// problems than they solve — a poison handler would loop
				// forever. Handlers needing retry should implement it
				// internally or persist to a retry queue.
			}
		}()

		if err := msg.Ack(); err != nil {
			span.RecordError(err)
			mlog.Error("failed to ack advisory", slog.Any("err", err))
		}
	}

	cctx, err := consumer.Consume(fn)
	if err != nil {
		return fmt.Errorf("start consume on %q: %w", consumerName, err)
	}

	m.mu.Lock()
	m.stopFuncs = append(m.stopFuncs, cctx.Stop)
	m.mu.Unlock()

	logger.Info("dlq consumer started")
	return nil
}

// fetchOriginal pulls the failed message's payload from its source stream.
//
// Returns:
//   - dlqMsg:        the envelope to pass to the handler, always populated
//   - payloadOK:     true if the original payload was successfully retrieved
//   - retry:         true if the caller should NOT ack (transient error;
//     JetStream will redeliver up to MaxDeliver times)
//
// Payload-lost case (payloadOK=false, retry=false): the original message
// aged out of the source stream before we could fetch it. We still hand the
// advisory to the handler so it can record the failure for audit, even
// without the payload.
func (m *Manager) fetchOriginal(
	ctx context.Context,
	advisory MaxDeliveriesAdvisory,
	logger *slog.Logger,
) (dlqMsg DLQMessage, payloadOK bool, retry bool) {
	// Skeleton envelope — populated whether or not we recover the payload.
	dlqMsg = DLQMessage{
		Advisory:   advisory,
		ReceivedAt: time.Now().UTC(),
	}

	streamCtx, cancel := context.WithTimeout(ctx, DLQFetchTimeout)
	defer cancel()

	origStream, err := m.Js.Stream(streamCtx, advisory.Stream)
	if err != nil {
		// Could be transient (NATS unreachable) or permanent (stream deleted).
		// We can't easily distinguish, so we ask for redelivery and rely on
		// MaxDeliver to stop the loop if the stream is truly gone.
		logger.Warn("failed to get source stream handle; will retry",
			slog.Any("err", err),
		)
		return dlqMsg, false, true
	}

	getCtx, cancel2 := context.WithTimeout(ctx, DLQFetchTimeout)
	defer cancel2()

	origMsg, err := origStream.GetMsg(getCtx, advisory.StreamSeq)
	if err != nil {
		if errors.Is(err, nats.ErrMsgNotFound) || errors.Is(err, jetstream.ErrMsgNotFound) {
			// Original aged out before we got here. This is normal under
			// short retention or heavy backlog — not an error. Hand the
			// advisory to the handler with PayloadLost=true so it can still
			// record the failure.
			logger.Info("original message no longer in source stream; passing advisory-only")
			dlqMsg.PayloadLost = true
			return dlqMsg, false, false
		}
		logger.Warn("failed to fetch original message; will retry",
			slog.Any("err", err),
		)
		return dlqMsg, false, true
	}

	dlqMsg.Subject = origMsg.Subject
	dlqMsg.Headers = origMsg.Header
	dlqMsg.Payload = origMsg.Data
	dlqMsg.TraceParent = headerCarrier(origMsg.Header).Get("traceparent")
	return dlqMsg, true, false
}

// GlobalDLQStreamName is the name of the shared DLQ stream that captures
// failures from many source streams. Exposed so callers can override it
// before startup if they want a different naming convention.
var GlobalDLQStreamName = "DLQ_GLOBAL"

// GlobalDLQConfig configures the multi-source DLQ consumer.
type GlobalDLQConfig struct {
	// Sources lists the source stream names whose advisories should be
	// captured. Each becomes a subject filter of the form
	//   $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<source>.>
	// on the shared DLQ stream.
	//
	// Leave empty AND set CaptureAll=true to capture advisories from every
	// stream in the account via a wildcard.
	Sources []string

	// CaptureAll, when true, uses a single wildcard subject to capture
	// advisories from every stream. Convenient but indiscriminate — every
	// MAX_DELIVERIES advisory in the account lands here. Prefer an explicit
	// Sources list unless you genuinely want firehose semantics.
	CaptureAll bool

	// Fallback handler invoked for advisories from sources without a
	// registered per-source handler. Required if you use RegisterHandler;
	// otherwise this is the handler for every message.
	Fallback DLQHandler

	// PerSource maps source stream name -> handler. Optional; if a source
	// has no entry here, Fallback is used.
	PerSource map[string]DLQHandler
}

// StartGlobalDLQConsumer wires up a single shared DLQ stream that captures
// MAX_DELIVERIES advisories from multiple source streams, with optional
// per-source handler routing.
//
// Use this instead of multiple StartDLQConsumer calls when you want
// centralized DLQ infrastructure: one stream to monitor, one consumer to
// scale, one place to alert. Trade-off: all sources share the same retention
// and stream-level limits.
//
// Idempotency note: as with the per-source consumer, handlers may be invoked
// more than once for the same advisory under transient failure. Dedup using
// Advisory.ID or (Advisory.Stream, Advisory.StreamSeq).
func (m *Manager) StartGlobalDLQConsumer(
	ctx context.Context,
	cfg GlobalDLQConfig,
) error {
	if cfg.Fallback == nil && len(cfg.PerSource) == 0 {
		return fmt.Errorf("global dlq: at least one of Fallback or PerSource must be set")
	}
	if !cfg.CaptureAll && len(cfg.Sources) == 0 {
		return fmt.Errorf("global dlq: must set Sources or CaptureAll=true")
	}

	logger := slog.Default().With(
		slog.String("component", "natsx.dlq"),
		slog.String("dlq_stream", GlobalDLQStreamName),
	)

	// Build subject filters. CaptureAll uses one wildcard; otherwise we
	// claim one subject per declared source. Note JetStream requires that
	// no two streams own the same subject — if another stream (e.g. a
	// per-source DLQ from StartDLQConsumer) already owns one of these,
	// CreateOrUpdateStream will fail. Pick one strategy per deployment.
	var subjects []string
	if cfg.CaptureAll {
		subjects = []string{"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"}
	} else {
		subjects = make([]string, 0, len(cfg.Sources))
		for _, src := range cfg.Sources {
			subjects = append(subjects,
				"$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES."+src+".>")
		}
	}

	// 1. Create / update the shared DLQ stream. Same retention semantics as
	// the per-source version: DiscardNew + bounded MaxAge/MaxBytes so
	// overflow is visible rather than silently lossy.
	stream, err := m.Js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        GlobalDLQStreamName,
		Description: "Shared dead letter queue capturing advisories from multiple source streams",
		Subjects:    subjects,
		Storage:     jetstream.FileStorage,
		Discard:     jetstream.DiscardNew,
		MaxAge:      DLQStreamMaxAge,
		MaxBytes:    DLQStreamMaxBytes,
		Metadata: map[string]string{
			"dead_letter_queue": "true",
			"scope":             "global",
		},
	})
	if err != nil {
		return fmt.Errorf("create global dlq stream: %w", err)
	}

	// 2. Single durable consumer fronting all sources.
	consumerName := GlobalDLQStreamName + "_handler"
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Name:        consumerName,
		Durable:     consumerName,
		AckPolicy:   jetstream.AckExplicitPolicy,
		AckWait:     DLQConsumerAckWait,
		MaxDeliver:  DLQConsumerMaxDeliver,
		Description: "Global DLQ handler consumer",
	})
	if err != nil {
		return fmt.Errorf("create global dlq consumer: %w", err)
	}

	// Copy the handler map so later mutations by the caller don't race
	// with consumer dispatch. If you need hot-reload of handlers, replace
	// this with a sync.RWMutex-protected map on the Manager.
	perSource := make(map[string]DLQHandler, len(cfg.PerSource))
	maps.Copy(perSource, cfg.PerSource)
	fallback := cfg.Fallback

	// 3. Dispatch loop. Each advisory is routed to the handler registered
	// for its source stream, or to the fallback handler. The bulk of the
	// per-message work — payload fetch, tracing, panic recovery — is
	// identical to the per-source consumer, so we delegate to the same
	// helpers.
	fn := func(msg jetstream.Msg) {
		msgCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var advisory MaxDeliveriesAdvisory
		if err := json.Unmarshal(msg.Data(), &advisory); err != nil {
			logger.Error("failed to unmarshal advisory; dropping",
				slog.Any("err", err),
				slog.String("data", string(msg.Data())),
			)
			_ = msg.Ack()
			return
		}

		mlog := logger.With(
			slog.String("advisory_id", advisory.ID),
			slog.String("advisory_stream", advisory.Stream),
			slog.String("advisory_consumer", advisory.Consumer),
			slog.Uint64("stream_seq", advisory.StreamSeq),
			slog.Uint64("deliveries", advisory.Deliveries),
		)

		// Route to per-source handler if registered, else fallback.
		handler, ok := perSource[advisory.Stream]
		if !ok {
			if fallback == nil {
				// No handler for this source and no fallback. Log loudly
				// and ack — sitting on un-acked messages would just trigger
				// MaxDeliver redelivery for advisories we structurally
				// can't process.
				mlog.Warn("no handler registered for source and no fallback; acking and dropping")
				_ = msg.Ack()
				return
			}
			handler = fallback
		}

		dlqMsg, payloadFetched, retry := m.fetchOriginal(msgCtx, advisory, mlog)
		if retry {
			return // un-acked → JetStream redelivers up to MaxDeliver
		}

		spanCtx := otel.GetTextMapPropagator().Extract(
			msgCtx,
			headerCarrier(dlqMsg.Headers),
		)
		// Only record a trace ID we actually inherited; otherwise the DLQ span is
		// a fresh root (or all zeros under a no-op tracer) and would mislead lookups.
		if parent := trace.SpanContextFromContext(spanCtx); parent.IsValid() {
			dlqMsg.TraceID = parent.TraceID().String()
		}
		spanCtx, span := otel.Tracer("nats.dlq").Start(
			spanCtx,
			"dlq.process."+advisory.Stream,
			trace.WithSpanKind(trace.SpanKindConsumer),
		)
		defer span.End()

		span.SetAttributes(
			attribute.String("dlq.stream", advisory.Stream),
			attribute.String("dlq.consumer", advisory.Consumer),
			attribute.String("dlq.scope", "global"),
			attribute.Int64("dlq.deliveries", int64(advisory.Deliveries)),
			attribute.Int64("dlq.stream_seq", int64(advisory.StreamSeq)),
			attribute.Bool("dlq.payload_lost", !payloadFetched),
		)

		func() {
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("handler panic: %v", r)
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
					mlog.Error("dlq handler panicked",
						slog.Any("panic", r),
						slog.String("trace_id", dlqMsg.TraceID),
					)
				}
			}()
			if err := handler(spanCtx, dlqMsg); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				mlog.Error("dlq handler returned error",
					slog.Any("err", err),
					slog.String("trace_id", dlqMsg.TraceID),
				)
			}
		}()

		if err := msg.Ack(); err != nil {
			span.RecordError(err)
			mlog.Error("failed to ack advisory", slog.Any("err", err))
		}
	}

	cctx, err := consumer.Consume(fn)
	if err != nil {
		return fmt.Errorf("start consume on %q: %w", consumerName, err)
	}

	m.mu.Lock()
	m.stopFuncs = append(m.stopFuncs, cctx.Stop)
	m.mu.Unlock()

	logger.Info("global dlq consumer started",
		slog.Int("sources", len(cfg.Sources)),
		slog.Bool("capture_all", cfg.CaptureAll),
		slog.Int("per_source_handlers", len(perSource)),
		slog.Bool("has_fallback", fallback != nil),
	)
	return nil
}
