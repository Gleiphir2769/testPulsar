package testPulsar

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"strconv"
	"testPulsar/lib/logger"
	"testPulsar/lib/sync/wait"
	"time"
)

func ProduceMsg(client pulsar.Client, wg *wait.Wait, topic string, count int) {
	defer wg.Done()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	if err != nil {
		log.Fatal(err)
	}

	if count == -1 {
		count = int(^uint(0) >> 1)
	}

	for i := 0; i < count; i++ {
		msg := []byte("msg" + strconv.Itoa(i))
		_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: msg,
		})
		if err != nil {
			logger.Error("Failed to publish message", err)
		}
		logger.Debug("Published message:", string(msg))
		time.Sleep(time.Millisecond * 100)
	}

	defer producer.Close()
}
