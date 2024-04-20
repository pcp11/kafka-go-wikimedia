package main

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/launchdarkly/eventsource"
)

func Produce() {
	// Create Producer Properties
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_URL"),
		"acks":              "all",
	}
	producer, err := kafka.NewProducer(configMap)
	topic := "wikimedia.recentchange"

	// Create the event handler
	eventHandler := func(e eventsource.Event) {
		// Assuming the handling logic is implemented here
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(e.Data()),
		}, nil)

		if err != nil {
			fmt.Printf("Delivery failed: %v\n", err)
		}
	}

	if err != nil {
		panic(err)
	}
	defer producer.Close()

	url := os.Getenv("WIKIMEDIA_URL")
	stream, err := eventsource.Subscribe(url, "")

	if err != nil {
		panic(err)
	}
	for {
		ev := <-stream.Events
		eventHandler(ev)
	}
}
