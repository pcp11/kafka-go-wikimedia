package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

const topic = "wikimedia.recentchange"

func Consume() {
	// Create Consumer Properties
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": os.Getenv("KAFKA_URL"),
		"group.id":          "group1",
		"auto.offset.reset": "smallest",
	}

	ctx := context.Background()
	consumer, err := kafka.NewConsumer(configMap)
	client, err := initOsClient(ctx)
	err = consumer.Subscribe(topic, nil)

	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	run := true

	for run == true {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			// application-specific processing

			//fmt.Printf("Received message on %s: %s\n", e.TopicPartition, string(e.Value))
			createDocument(ctx, client, e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}
}

func extractId(jsonBytes []byte) (string, error) {
	var data struct {
		Meta struct {
			ID string `json:"id"`
		} `json:"meta"`
	}
	err := json.Unmarshal(jsonBytes, &data)

	if err != nil {
		return "", err
	}
	return data.Meta.ID, nil
}

func createDocument(ctx context.Context, client *opensearchapi.Client, message *kafka.Message) {
	id, err := extractId(message.Value)

	if err != nil {
		fmt.Printf("Error unmarshalling JSON: %s\n", err.Error())
	}
	fmt.Println(id)

	_, err = client.Index(
		ctx,
		opensearchapi.IndexReq{
			Index:      "wikimedia",
			DocumentID: id,
			Body:       bytes.NewReader(message.Value),
			Params: opensearchapi.IndexParams{
				Refresh: "true",
			},
		},
	)

	if err != nil {
		fmt.Printf("Error sending document to Opensearch: %s\n", err.Error())
	}
}

func initOsClient(ctx context.Context) (*opensearchapi.Client, error) {
	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // For testing only. Use certificate for validation.
				},
				Addresses: []string{os.Getenv("OPENSEARCH_URL")},
				Username:  "admin",
				Password:  "admin",
			},
		})

	if err != nil {
		return nil, err
	}
	_, err = client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{Indices: []string{"wikimedia"}})

	if err != nil {
		if messageErr, ok := err.(*opensearch.MessageError); ok && messageErr.Status == "404" {
			_, err := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{Index: "wikimedia"})

			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	return client, nil
}
