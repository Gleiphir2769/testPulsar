package testPulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"strconv"
	"testPulsar/lib/logger"
	"testPulsar/lib/sync/wait"
)

func ProduceMsg(client pulsar.Client, msgNum int, wg *wait.Wait, stopChan chan bool) {
	defer wg.Done()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-topic",
	})

	if err != nil {
		log.Fatal(err)
	}

	for i:=0; i<msgNum; i++ {
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("msg" + strconv.Itoa(i)),
		})
		if err != nil {
			logger.Error("Failed to publish message", err)
		}
	}

	stopChan <- true

	defer producer.Close()


	logger.Debug("Published message")
}

func TestProduceMsg(client pulsar.Client) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "my-topic",
	})

	if err != nil {
		log.Fatal(err)
	}

	for i:=0; i<10; i++ {
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("msg" + strconv.Itoa(i)),
		})
		if err != nil {
			logger.Error("Failed to publish message", err)
		}
	}

	defer producer.Close()


	logger.Debug("Published message")
}