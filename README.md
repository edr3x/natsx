# natsx

A small wrapper around [nats.go](https://github.com/nats-io/nats.go) and JetStream that covers the setup most services need:

- **One managed connection.** A `Manager` holds the NATS connection and the JetStream context, reconnects on its own, and shuts down cleanly.
- **Tracing built in.** `Publish` adds OpenTelemetry trace context to message headers, and `Subscribe` reads it back, so a trace follows a message from one service to the next.
- **Consumers with sensible defaults.** You get durable pull consumers with explicit ack, retry with backoff, and a way to mark errors as not worth retrying.
- **Dead letter queues (DLQ).** Messages that fail every retry are captured, fetched again from their stream, and passed to your handler, either per stream or through one shared DLQ.

natsx does **not** create your streams or define your subjects. You create streams yourself (for example with `mgr.Js.CreateOrUpdateStream`).

```sh
go get github.com/edr3x/natsx
```

## Quick start

```go
mgr, err := natsx.New() // uses NATS_URL, or nats://127.0.0.1:4222 if unset
if err != nil {
    log.Fatal(err)
}
defer mgr.Close()

// Publish
if err := mgr.Publish(ctx, "orders.created", data); err != nil {
    return err
}

// Consume
stream, _ := mgr.Js.Stream(ctx, "orders")
consumer, err := (&natsx.ConsumerSpec{Name: "billing", Subject: "orders.>"}).
    RegisterConsumer(ctx, stream)
if err != nil {
    return err
}
err = mgr.Subscribe(consumer, func(ctx context.Context, msg jetstream.Msg) error {
    return handle(ctx, msg.Data()) // return nil and natsx acks the message for you
})
```

## Manager

```go
mgr, err := natsx.New(
    natsx.WithURL("nats://nats:4222"),  // default: $NATS_URL, then nats.DefaultURL
    natsx.WithConsumerPrefetch(10),     // default: 1 (one message in flight per consumer)
)
```

- `New` is an alias for `NewEventManager`. Each call opens a **new** connection.
- `mgr.Nc` (`*nats.Conn`) and `mgr.Js` (`jetstream.JetStream`) are exported. Use them for anything natsx doesn't wrap: streams, KV buckets, request/reply.
- The connection retries every 2s, up to 10 times. Disconnects, reconnects and errors are logged through `slog`.
- `Close()` stops every consumer started through the manager, drains the connection, then closes it. Call it once at shutdown. Calling it on a nil `*Manager` does nothing.

## Publishing

```go
mgr.Publish(ctx, subject, data)
mgr.PublishWithHeaders(ctx, subject, data, map[string][]string{"Dlq-Replay-Of": {id}})
```

Both publish through JetStream, start a producer span named `nats.publish <subject>`, and add the trace context from `ctx` to the headers. Extra headers are merged in after the trace headers, so a key in `extraHeaders` overwrites a trace header with the same name.

## Consuming

### ConsumerSpec

```go
spec := &natsx.ConsumerSpec{
    Name:       "billing",       // required; also used as the durable name
    Subject:    "orders.>",      // required; filter subject
    MaxDeliver: 5,               // default 5
    AckWait:    time.Minute,     // default 1m; keep it >= the largest backoff step
}
consumer, err := spec.RegisterConsumer(ctx, stream)
```

The consumer is durable, uses explicit ack, and retries after 5s, 10s, 15s and 20s. `RegisterConsumer` creates the consumer or updates it in place, so it is safe to call on every startup.

To take full control, pass a `jetstream.ConsumerConfig` as the third argument. It **replaces** the generated config entirely; nothing is merged.

```go
spec.RegisterConsumer(ctx, stream, jetstream.ConsumerConfig{ /* ... */ })
```

### Subscribe and handler results

`mgr.Subscribe(consumer, handler)` delivers messages in the background until `Close()` is called. What happens to a message depends on what the handler returns:

| Handler returns | natsx does | Result |
|---|---|---|
| `nil` | `Ack()` | Done |
| an ordinary error | nothing (no ack) | Redelivered after the backoff, up to `MaxDeliver` times |
| `natsx.NewTerminalError(err)` | `Term()` | Dropped, never retried, and never sent to the DLQ |

Return a terminal error for failures that will never succeed on retry, such as a payload that can't be decoded or data that fails validation:

```go
if err := json.Unmarshal(msg.Data(), &order); err != nil {
    return natsx.NewTerminalError(err) // or natsx.NewTerminalErrorf("bad order: %w", err)
}
```

`natsx.IsTerminalError(err)` checks for one, including when it is wrapped inside another error.

Each message is handled in a consumer span named `nats.consume <subject>`, linked to the trace of whoever published it. The span records `nats.delivery_count`, `nats.stream_seq`, `nats.subject` and `nats.stream`. The handler's `ctx` comes from `context.Background()`, not from the caller, so a handler that is already running keeps going even if the code that called `Subscribe` has finished. Trace headers are found whether they were sent as `Traceparent` (Go clients) or `traceparent` (the nats CLI and other languages).

## Dead letter queues

When a message fails `MaxDeliver` times, JetStream publishes a `MAX_DELIVERIES` notice (an "advisory") that says which stream and sequence number failed. natsx stores these notices in their own DLQ stream, fetches the original message from its source stream, and calls your handler with a `DLQMessage`:

```go
type DLQMessage struct {
    Advisory    MaxDeliveriesAdvisory // stream, consumer, stream_seq, deliveries, timestamp, id
    Subject     string
    Headers     nats.Header
    Payload     []byte
    PayloadLost bool      // original already expired from its source stream; only Advisory is set
    TraceParent string
    TraceID     string    // set only if the original message carried trace context
    ReceivedAt  time.Time // when natsx received the advisory; the failure time is Advisory.Timestamp
}
```

### One DLQ per stream

```go
err := mgr.StartDLQConsumer(ctx, "orders", func(ctx context.Context, m natsx.DLQMessage) error {
    return store.SaveFailure(ctx, m)
})
```

This creates a stream `orders_dlq` and a durable consumer `orders_dlq_handler` on it.

### One shared DLQ

```go
err := mgr.StartGlobalDLQConsumer(ctx, natsx.GlobalDLQConfig{
    Sources:   []string{"orders", "payments"}, // or CaptureAll: true for every stream in the account
    PerSource: map[string]natsx.DLQHandler{"payments": handlePaymentFailure},
    Fallback:  handleAnyFailure,
})
```

This creates one stream, `DLQ_GLOBAL` (rename it with `natsx.GlobalDLQStreamName`), and routes each failure to the `PerSource` handler for its stream, or to `Fallback` if there isn't one. You must set `Sources` or `CaptureAll`, and at least one of `PerSource` or `Fallback`. If a failure has no matching handler and no fallback, it is logged and dropped.

**Pick one approach per deployment.** In JetStream, only one stream can own a given subject. A per-stream DLQ and a shared DLQ that both try to capture the same source will conflict, and the second one fails to start.

### How DLQ handlers behave

- **Handlers must be idempotent.** The same advisory can be delivered more than once after a network blip or restart. Use `Advisory.ID`, or the pair `Advisory.Stream` + `Advisory.StreamSeq`, as the dedup key.
- **Errors and panics are logged and recorded on the span, but the advisory is still acked.** This stops one bad handler from retrying forever. If you need retries, build them into the handler or send the message to your own retry queue.
- **A failure to fetch the original message** (source stream unreachable, timeout) is retried, up to `DLQConsumerMaxDeliver` times.
- **If the original has already expired** from its source stream, the handler still runs, with `PayloadLost = true`. Record the advisory anyway so the failure isn't lost.
- **Advisories that can't be parsed** are logged and dropped.
- **Stopping:** DLQ consumers stop when `ctx` is cancelled or `Close()` is called.

### Tuning

These are package variables. Set them before starting any DLQ consumer:

| Variable | Default | Meaning |
|---|---|---|
| `DLQStreamMaxAge` | 30 days | How long advisories are kept in the DLQ stream |
| `DLQStreamMaxBytes` | 1 GiB | Size cap for the DLQ stream. When full, new advisories are **rejected** and old ones are kept, so overflow shows up as errors instead of silent data loss |
| `DLQConsumerMaxDeliver` | 5 | How many times one advisory is retried |
| `DLQConsumerAckWait` | 30s | How long JetStream waits for an ack before redelivering an advisory |
| `DLQFetchTimeout` | 5s | Timeout for each NATS call made while fetching the original message |

To replay a failed message, publish `m.Payload` to `m.Subject` with `PublishWithHeaders`. Add a correlation header such as `Dlq-Replay-Of`, set to the advisory ID, so the replay can be traced back to the original failure.

## Package-level shortcuts

For services that only ever need one connection:

```go
natsx.Publish(ctx, subject, data)
natsx.PublishWithHeaders(ctx, subject, data, headers)
natsx.Subscribe(ctx, "orders", "billing", func(ctx context.Context, data []byte) error { ... })
```

These use a default manager that is created on first use from `NATS_URL`. `natsx.Subscribe` looks up an **existing** stream and consumer; it doesn't create them. To use your own configured manager instead, call `natsx.SetDefaultManager(mgr)` before any shortcut runs. It isn't safe to call while other goroutines are using the shortcuts.

## Tracing setup

natsx uses the global OpenTelemetry tracer and propagator, so set a propagator once at startup. Without one, no trace context is written to or read from headers:

```go
otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
    propagation.TraceContext{}, propagation.Baggage{},
))
```

## Testing

```sh
go test ./...                    # unit tests and examples
go test -tags integration ./...  # needs a NATS server with JetStream (NATS_URL); skipped if none is reachable
```

To start a local server: `docker run -p 4222:4222 nats -js`
