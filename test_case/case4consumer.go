package test_case

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
)

func Case3(brokers string, topic string, sub string, count int) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: brokers,
	})

	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Only subscribe on the specific partition
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
	})

	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	if count == -1 {
		count = int(^uint(0) >> 1)
	}

	for i := 0; i < count; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}
}
