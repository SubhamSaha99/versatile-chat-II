package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"versatile-chat-II/internal/modules/message"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	consumerPollWait = 250 * time.Millisecond
	flushTimeout     = 15 * time.Second
)

type MessageConsumer struct {
	client       *kgo.Client
	repository   *message.MessageRepository
	flushEvery   time.Duration
	maxBatchSize int
}

func NewMessageConsumer(
	brokers string,
	topic string,
	groupID string,
	repository *message.MessageRepository,
	flushEvery time.Duration,
	maxBatchSize int,
) (*MessageConsumer, error) {
	var seedBrokers []string
	for _, broker := range strings.Split(brokers, ",") {
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
	if strings.TrimSpace(groupID) == "" {
		return nil, fmt.Errorf("Kafka consumer group must be configured")
	}
	if repository == nil {
		return nil, fmt.Errorf("message repository must be configured")
	}
	if flushEvery <= 0 {
		return nil, fmt.Errorf("message flush interval must be greater than zero")
	}
	if maxBatchSize <= 0 {
		return nil, fmt.Errorf("message batch size must be greater than zero")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(seedBrokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(strings.TrimSpace(topic)),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(consumerPollWait),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka message consumer: %w", err)
	}

	return &MessageConsumer{
		client:       client,
		repository:   repository,
		flushEvery:   flushEvery,
		maxBatchSize: maxBatchSize,
	}, nil
}

// Run consumes records until ctx is canceled. It writes each batch to MongoDB
// before committing the corresponding Kafka offsets.
func (c *MessageConsumer) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.flushEvery)
	defer ticker.Stop()

	messages := make([]message.Message, 0, c.maxBatchSize)
	records := make([]*kgo.Record, 0, c.maxBatchSize)

	flush := func(flushCtx context.Context) error {
		if len(records) == 0 {
			return nil
		}

		writeCtx, cancel := context.WithTimeout(flushCtx, flushTimeout)
		defer cancel()

		if err := c.repository.BulkUpsert(writeCtx, messages); err != nil {
			return fmt.Errorf("persist message batch: %w", err)
		}
		if err := c.client.CommitRecords(writeCtx, records...); err != nil {
			return fmt.Errorf("commit Kafka message offsets: %w", err)
		}

		slog.Info("Persisted Kafka message batch", "count", len(messages))
		messages = messages[:0]
		records = records[:0]
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			flushCtx, cancel := context.WithTimeout(context.Background(), flushTimeout)
			defer cancel()
			if flushErr := flush(flushCtx); flushErr != nil {
				return flushErr
			}
			return nil
		}

		remaining := c.maxBatchSize - len(records)
		fetches := c.client.PollRecords(ctx, remaining)
		if err := ctx.Err(); err != nil {
			continue
		}
		for _, fetchErr := range fetches.Errors() {
			slog.Warn("Kafka message fetch failed", "topic", fetchErr.Topic, "partition", fetchErr.Partition, "error", fetchErr.Err)
		}

		var decodeErr error
		fetches.EachRecord(func(record *kgo.Record) {
			var message message.Message
			if err := json.Unmarshal(record.Value, &message); err != nil {
				decodeErr = fmt.Errorf("decode Kafka message at partition %d offset %d: %w", record.Partition, record.Offset, err)
				return
			}
			if message.ID.IsZero() || message.SenderID.IsZero() || message.ReceiverID.IsZero() || strings.TrimSpace(message.Body) == "" {
				decodeErr = fmt.Errorf("invalid Kafka message at partition %d offset %d", record.Partition, record.Offset)
				return
			}
			messages = append(messages, message)
			records = append(records, record)
		})
		if decodeErr != nil {
			return decodeErr
		}

		if len(records) >= c.maxBatchSize {
			if err := flush(ctx); err != nil {
				return err
			}
			continue
		}

		select {
		case <-ticker.C:
			if err := flush(ctx); err != nil {
				return err
			}
		default:
		}
	}
}

func (c *MessageConsumer) Close() {
	c.client.Close()
}
