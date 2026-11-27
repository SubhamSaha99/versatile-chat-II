package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaProducer struct {
	client *kgo.Client
	topic  string
}

func NewKafkaProducer(brokers, topic string) (*KafkaProducer, error) {
	var seedBrokers []string
	for broker := range strings.SplitSeq(brokers, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			seedBrokers = append(seedBrokers, broker)
		}
	}
	if len(seedBrokers) == 0 {
		return nil, fmt.Errorf("at least one Kafka broker must be configured")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, fmt.Errorf("Kafka topic must be configured")
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(seedBrokers...))
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}

	return &KafkaProducer{client: client, topic: topic}, nil
}

func (p *KafkaProducer) Publish(ctx context.Context, key string, payload []byte) error {
	result := p.client.ProduceSync(ctx, &kgo.Record{
		Topic: p.topic,
		Key:   []byte(key),
		Value: payload,
	}).FirstErr()
	if result != nil {
		return fmt.Errorf("produce Kafka message: %w", result)
	}
	return nil
}

func (p *KafkaProducer) Close() {
	p.client.Close()
}
