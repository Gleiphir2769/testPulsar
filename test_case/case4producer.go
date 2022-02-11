package test_case

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"strconv"
	"testPulsar/lib/logger"
	"time"
)

func Case2(brokers string, topic string, count int) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               brokers,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	if err != nil {
		log.Fatal(err)
	}

	if count == -1 {
		count = int(^uint(0) >> 1)
	}

	clusters := []string{"pulsar-cluster-1", "pulsar-cluster-2"}

	for i := 0; i < count; i++ {
		msg := []byte("msg" + strconv.Itoa(i))
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload:             msg,
			ReplicationClusters: clusters,
		})
		if err != nil {
			logger.Error("Failed to publish message", err)
		}
		logger.Debug("Published message:", string(msg))
		time.Sleep(time.Millisecond * 100)
	}

	defer producer.Close()
}
