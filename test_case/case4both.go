package test_case

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
	"testPulsar"
	"testPulsar/lib/logger"
	"testPulsar/lib/sync/wait"
	"time"
)

func Case1(brokers string, topic string, sub string, count int) {

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               brokers,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	defer client.Close()

	var ok bool

	wg := &wait.Wait{}
	wg.Add(2)
	start := time.Now()
	go testPulsar.ProduceMsg(client, wg, topic, count)
	ok = testPulsar.ConsumeMsg(client, wg, topic, sub, count)
	cost := time.Since(start)
	logger.Debug(fmt.Sprintf("cost=[%s]\n", cost))
	logger.Debug("test result:", ok)
}
