package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// Producer handles message production to Kafka
type Producer struct {
	producer sarama.SyncProducer
	config   TestConfig
	logger   *log.Logger
}

// NewProducer creates a new Kafka producer
func NewProducer(config TestConfig, logger *log.Logger) (*Producer, error) {
	// Configure Sarama producer for ordering guarantees
	saramaConfig := sarama.NewConfig()

	// CRITICAL: Enable idempotence to prevent duplicates and ensure ordering
	saramaConfig.Producer.Idempotent = true
	saramaConfig.Net.MaxOpenRequests = 1

	// CRITICAL: Max in-flight requests = 1 ensures strict ordering within partition
	// Setting this to 5 would allow out-of-order messages in case of retries
	saramaConfig.Producer.MaxMessageBytes = 1000000

	// Require acknowledgment from all in-sync replicas for durability
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll

	// Return successes so we can track what was sent
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	// Retry configuration
	saramaConfig.Producer.Retry.Max = 3

	// Partitioner: We'll use manual partitioning based on partition key
	saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner

	// Version
	saramaConfig.Version = sarama.V2_6_0_0

	producer, err := sarama.NewSyncProducer(config.KafkaBrokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return &Producer{
		producer: producer,
		config:   config,
		logger:   logger,
	}, nil
}

// ProduceMessages sends numbered messages to all partitions
func (p *Producer) ProduceMessages() (int, error) {
	p.logger.Printf("Starting to produce %d messages to topic '%s' with %d partitions",
		p.config.NumMessages, p.config.TopicName, p.config.NumPartitions)

	// Track sequence numbers per partition
	sequenceNums := make(map[int32]int)
	for i := 0; i < p.config.NumPartitions; i++ {
		sequenceNums[int32(i)] = 0
	}

	successCount := 0
	errorCount := 0

	// Send messages in round-robin fashion across partitions
	for i := 0; i < p.config.NumMessages; i++ {
		partition := int32(i % p.config.NumPartitions)
		partitionKey := fmt.Sprintf("key-%d", partition)

		msg := TestMessage{
			PartitionKey: partitionKey,
			SequenceNum:  sequenceNums[partition],
			Timestamp:    time.Now().UnixNano(),
			MessageID:    fmt.Sprintf("%s-seq-%d", partitionKey, sequenceNums[partition]),
		}

		// Increment sequence number for this partition
		sequenceNums[partition]++

		// Serialize message to JSON
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			p.logger.Printf("ERROR: Failed to marshal message: %v", err)
			errorCount++
			continue
		}

		// Create Kafka message
		kafkaMsg := &sarama.ProducerMessage{
			Topic:     p.config.TopicName,
			Key:       sarama.StringEncoder(partitionKey),
			Value:     sarama.ByteEncoder(msgBytes),
			Partition: partition, // Explicitly set partition
		}

		// Send message synchronously
		partition, offset, err := p.producer.SendMessage(kafkaMsg)
		if err != nil {
			p.logger.Printf("ERROR: Failed to send message %s: %v", msg.MessageID, err)
			errorCount++
			continue
		}

		successCount++
		p.logger.Printf("PRODUCED: Partition=%d, Offset=%d, Key=%s, Seq=%d, MsgID=%s",
			partition, offset, msg.PartitionKey, msg.SequenceNum, msg.MessageID)

		// Small delay between messages to simulate real-world scenario
		if p.config.MessageDelay > 0 {
			time.Sleep(p.config.MessageDelay)
		}
	}

	p.logger.Printf("Finished producing messages.")
	p.logger.Printf("  Successfully sent: %d messages", successCount)
	p.logger.Printf("  Failed: %d messages", errorCount)
	for partition, seq := range sequenceNums {
		p.logger.Printf("  Partition %d: %d messages (seq 0-%d)", partition, seq, seq-1)
	}

	if errorCount > 0 {
		return successCount, fmt.Errorf("failed to send %d messages", errorCount)
	}

	return successCount, nil
}

// Close closes the producer
func (p *Producer) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}
