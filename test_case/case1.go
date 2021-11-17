package test_case

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"testPulsar"
	"testPulsar/lib/logger"
	"testPulsar/lib/sync/wait"
	"time"
)


func Case1(brokers string) {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               brokers,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	defer client.Close()

	stopChan := make(chan bool)
	var ok bool

	wg := &wait.Wait{}
	wg.Add(2)
	start := time.Now()
	go testPulsar.ProduceMsg(client, 10000, wg, stopChan)
	ok = testPulsar.ConsumeMsg(client, 10000, wg)
	cost := time.Since(start)
	logger.Debug(fmt.Sprintf("cost=[%s]\n", cost))
	logger.Debug("test result:", ok)
}

func Case2(brokers string) {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               brokers,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	defer client.Close()

	start := time.Now()
	testPulsar.TestProduceMsg(client)
	cost := time.Since(start)
	logger.Debug(fmt.Sprintf("cost=[%s]\n", cost))
}

func Case3(brokers string) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: brokers,
	})

	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Only subscribe on the specific partition
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "my-topic",
		SubscriptionName: "my-sub",
	})

	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}
}