// Package kafkax wraps segmentio/kafka-go with a small, test-friendly
// interface so handlers can be unit-tested without a real broker.
package kafkax

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/airwallex/heartbeat/pkg/model"
)

// Producer is the minimal surface ingestion depends on. The concrete
// KafkaProducer satisfies this; tests inject a fake.
type Producer interface {
	Publish(ctx context.Context, events []model.Heartbeat) error
	Close() error
}

// Config holds the parameters for the real Kafka-backed producer.
type Config struct {
	Brokers      []string
	Topic        string
	BatchTimeout time.Duration
	BatchSize    int
	RequiredAcks kafka.RequiredAcks
}

// KafkaProducer is a thin wrapper around kafka.Writer that speaks in
// domain types.
type KafkaProducer struct {
	w *kafka.Writer
}

// NewProducer constructs a KafkaProducer with sensible defaults.
//
// We key messages by DeviceID so a single device's events land on one
// partition - required for correct per-device ordering and for HLL
// state locality on the Flink side.
func NewProducer(cfg Config) *KafkaProducer {
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.BatchTimeout == 0 {
		cfg.BatchTimeout = 20 * time.Millisecond
	}
	if cfg.RequiredAcks == 0 {
		cfg.RequiredAcks = kafka.RequireOne
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		RequiredAcks: cfg.RequiredAcks,
		Async:        false,
	}
	return &KafkaProducer{w: w}
}

// Publish JSON-encodes and writes events as a single batch.
func (p *KafkaProducer) Publish(ctx context.Context, events []model.Heartbeat) error {
	if len(events) == 0 {
		return nil
	}
	msgs := make([]kafka.Message, 0, len(events))
	for i := range events {
		b, err := json.Marshal(events[i])
		if err != nil {
			return fmt.Errorf("encode heartbeat %d: %w", i, err)
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(events[i].DeviceID),
			Value: b,
			Time:  time.UnixMilli(events[i].TimestampMs),
		})
	}
	return p.w.WriteMessages(ctx, msgs...)
}

func (p *KafkaProducer) Close() error { return p.w.Close() }
