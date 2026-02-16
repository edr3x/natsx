package natsx

import (
	"context"
	"log"

	"github.com/nats-io/nats.go/jetstream"
)

func ExampleNew() {
	ctx := context.Background()

	mgr, err := New()
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Close()

	if err := mgr.Publish(ctx, "subject", []byte("message")); err != nil {
		log.Fatal(err)
	}
}

func ExampleConsumerSpec_RegisterConsumer() {
	ctx := context.Background()

	mgr, err := New()
	if err != nil {
		log.Fatal(err)
	}
	defer mgr.Close()

	stream, err := mgr.Js.Stream(ctx, "orders")
	if err != nil {
		log.Fatal(err)
	}

	spec := &ConsumerSpec{
		Name:    "my-consumer",
		Subject: "orders.>",
	}

	consumer, err := spec.RegisterConsumer(ctx, stream)
	if err != nil {
		log.Fatal(err)
	}

	err = mgr.Subscribe(consumer, func(ctx context.Context, msg jetstream.Msg) error {
		return msg.Ack()
	})
	if err != nil {
		log.Fatal(err)
	}
}
