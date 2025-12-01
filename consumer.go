package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Consumer handles message consumption from Kafka
type Consumer struct {
	consumerID    string
	config        TestConfig
	logger        *log.Logger
	results       *TestResults
	verifier      *OrderVerifier
	consumerGroup sarama.ConsumerGroup
	ready         chan bool
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(consumerID string, config TestConfig, logger *log.Logger, results *TestResults, verifier *OrderVerifier) (*Consumer, error) {
	// Configure Sarama consumer
	saramaConfig := sarama.NewConfig()

	// Version
	saramaConfig.Version = sarama.V2_6_0_0

	// CRITICAL: Start consuming from the beginning of the topic
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	// CRITICAL: Enable auto-commit but with a reasonable interval
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	// Consumer group rebalance strategy
	// - Range: Assigns partitions in ranges (default)
	// - RoundRobin: Distributes partitions evenly
	// - Sticky: Tries to maintain previous assignments during rebalance
	saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()

	// Session timeout: If consumer doesn't send heartbeat within this time, rebalance
	saramaConfig.Consumer.Group.Session.Timeout = 10 * time.Second

	// Heartbeat interval: How often to send heartbeats
	saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	// Rebalance timeout
	saramaConfig.Consumer.Group.Rebalance.Timeout = 60 * time.Second

	// Return errors
	saramaConfig.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup(config.KafkaBrokers, config.ConsumerGroup, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		consumerID:    consumerID,
		config:        config,
		logger:        logger,
		results:       results,
		verifier:      verifier,
		consumerGroup: consumerGroup,
		ready:         make(chan bool),
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// Start begins consuming messages
func (c *Consumer) Start(wg *sync.WaitGroup) {
	defer wg.Done()

	handler := &consumerGroupHandler{
		consumer: c,
	}

	c.logger.Printf("[%s] Consumer starting...", c.consumerID)

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			default:
				// Consume joins the consumer group and starts consuming
				// This call blocks until the context is cancelled
				if err := c.consumerGroup.Consume(c.ctx, []string{c.config.TopicName}, handler); err != nil {
					c.logger.Printf("[%s] ERROR: Consumer error: %v", c.consumerID, err)
				}

				// Check if context was cancelled
				if c.ctx.Err() != nil {
					return
				}
			}
		}
	}()

	// Wait for consumer to be ready
	<-c.ready
	c.logger.Printf("[%s] Consumer is ready", c.consumerID)
}

// Stop stops the consumer
func (c *Consumer) Stop() error {
	c.logger.Printf("[%s] Consumer stopping...", c.consumerID)
	c.cancel()
	return c.consumerGroup.Close()
}

// consumerGroupHandler implements sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	consumer *Consumer
}

// Setup is called at the beginning of a new session, before ConsumeClaim
func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	partitions := session.Claims()[h.consumer.config.TopicName]

	h.consumer.logger.Printf("[%s] REBALANCE: SETUP - Assigned partitions: %v (Generation: %d)",
		h.consumer.consumerID, partitions, session.GenerationID())

	// Record rebalance event
	event := RebalanceEvent{
		Timestamp:      time.Now(),
		EventType:      "ASSIGN",
		ConsumerID:     h.consumer.consumerID,
		Partitions:     partitions,
		PartitionCount: len(partitions),
	}
	h.consumer.results.AddRebalanceEvent(event)

	// Signal that consumer is ready
	select {
	case h.consumer.ready <- true:
	default:
	}

	return nil
}

// Cleanup is called at the end of a session, once all ConsumeClaim goroutines have exited
func (h *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	partitions := session.Claims()[h.consumer.config.TopicName]

	h.consumer.logger.Printf("[%s] REBALANCE: CLEANUP - Revoking partitions: %v (Generation: %d)",
		h.consumer.consumerID, partitions, session.GenerationID())

	// Record rebalance event
	event := RebalanceEvent{
		Timestamp:      time.Now(),
		EventType:      "REVOKE",
		ConsumerID:     h.consumer.consumerID,
		Partitions:     partitions,
		PartitionCount: len(partitions),
	}
	h.consumer.results.AddRebalanceEvent(event)

	return nil
}

// ConsumeClaim processes messages from a partition claim
func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.consumer.logger.Printf("[%s] Started consuming partition %d from offset %d",
		h.consumer.consumerID, claim.Partition(), claim.InitialOffset())

	// Process messages
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Parse message
			var testMsg TestMessage
			if err := json.Unmarshal(message.Value, &testMsg); err != nil {
				h.consumer.logger.Printf("[%s] ERROR: Failed to unmarshal message: %v",
					h.consumer.consumerID, err)
				session.MarkMessage(message, "")
				continue
			}

			// Create consumed message
			consumedMsg := ConsumedMessage{
				Message:    testMsg,
				ConsumerID: h.consumer.consumerID,
				Partition:  message.Partition,
				Offset:     message.Offset,
				ConsumedAt: time.Now(),
			}

			// Log consumption
			h.consumer.logger.Printf("[%s] CONSUMED: Partition=%d, Offset=%d, Key=%s, Seq=%d, MsgID=%s",
				h.consumer.consumerID, message.Partition, message.Offset,
				testMsg.PartitionKey, testMsg.SequenceNum, testMsg.MessageID)

			// Verify message ordering
			h.consumer.verifier.VerifyMessage(consumedMsg)

			// Mark message as processed
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
