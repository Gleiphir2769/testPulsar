package testPulsar

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"strconv"
	"testPulsar/lib/logger"
	"testPulsar/lib/sync/wait"
)

func ConsumeMsg(client pulsar.Client, msgNum int, wg *wait.Wait) bool {
	defer wg.Done()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            "my-topic",
		SubscriptionName: "my-sub",
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	count := 0

	if err != nil {
		logger.Error("create file failed: ", err)
	}


	for  {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		if count == 0 {
			count, _ = strconv.Atoi(string(msg.Payload())[3:])
		}

		output := fmt.Sprintf("Received message msgId: %#v -- content: '%s' -- count: %d\n",
			msg.ID(), string(msg.Payload()), count)

		logger.Debug(output)

		consumer.Ack(msg)
		count ++

		if count >= msgNum {
			break
		}
	}


	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}

	if count != msgNum && msgNum != -1 {
		logger.Error("count [%d] is not matched msgNum [%d]", count, msgNum)
		return false
	}
	return true
}