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
	// Use configured value or default to 10 seconds
	sessionTimeout := config.SessionTimeout
	if sessionTimeout == 0 {
		sessionTimeout = 10 * time.Second
	}
	saramaConfig.Consumer.Group.Session.Timeout = sessionTimeout

	// Heartbeat interval: How often to send heartbeats
	// Use configured value or default to 3 seconds
	heartbeatInterval := config.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 3 * time.Second
	}
	saramaConfig.Consumer.Group.Heartbeat.Interval = heartbeatInterval

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

	// Check for in-flight messages and mark them as interrupted
	inFlight := h.consumer.results.GetInFlightMessages()
	if len(inFlight) > 0 {
		h.consumer.logger.Printf("[%s] REBALANCE: INTERRUPTION - %d messages were in-flight during rebalance",
			h.consumer.consumerID, len(inFlight))

		for _, msg := range inFlight {
			// Only interrupt messages being processed by this consumer
			if msg.ConsumerID == h.consumer.consumerID {
				h.consumer.results.InterruptProcessing(msg.MessageID)
				h.consumer.logger.Printf("[%s] INTERRUPTED: Partition=%d, Offset=%d, Seq=%d, MsgID=%s (was processing for %s)",
					h.consumer.consumerID, msg.Partition, msg.Offset, msg.SequenceNum, msg.MessageID,
					time.Since(msg.ProcessingStarted).Round(time.Millisecond))
			}
		}
	}

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

			// === START PROCESSING SIMULATION ===
			// Mark message as started processing
			h.consumer.results.StartProcessing(
				testMsg.MessageID,
				message.Partition,
				message.Offset,
				testMsg.SequenceNum,
				h.consumer.consumerID,
			)

			h.consumer.logger.Printf("[%s] PROCESSING START: Partition=%d, Offset=%d, Seq=%d, MsgID=%s",
				h.consumer.consumerID, message.Partition, message.Offset,
				testMsg.SequenceNum, testMsg.MessageID)

			// Simulate realistic message processing with configurable delay
			processingTime := h.consumer.calculateProcessingTime()

			// Use a timer to simulate processing - this can be interrupted by rebalance
			processingTimer := time.NewTimer(processingTime)

			select {
			case <-processingTimer.C:
				// Processing completed normally
				h.consumer.results.CompleteProcessing(testMsg.MessageID, h.consumer.consumerID)

				h.consumer.logger.Printf("[%s] PROCESSING COMPLETE: Partition=%d, Offset=%d, Seq=%d, MsgID=%s (took %s)",
					h.consumer.consumerID, message.Partition, message.Offset,
					testMsg.SequenceNum, testMsg.MessageID, processingTime.Round(time.Millisecond))

				// Mark message as processed (commit offset)
				session.MarkMessage(message, "")

			case <-session.Context().Done():
				// Rebalance occurred during processing - message will be marked as interrupted in Cleanup
				processingTimer.Stop()
				h.consumer.logger.Printf("[%s] PROCESSING ABORTED: Partition=%d, Offset=%d, Seq=%d, MsgID=%s (rebalance triggered)",
					h.consumer.consumerID, message.Partition, message.Offset,
					testMsg.SequenceNum, testMsg.MessageID)
				return nil
			}
			// === END PROCESSING SIMULATION ===

		case <-session.Context().Done():
			return nil
		}
	}
}

// calculateProcessingTime returns a processing time with jitter
func (c *Consumer) calculateProcessingTime() time.Duration {
	minTime := c.config.ProcessingTimeMin
	maxTime := c.config.ProcessingTimeMax

	// Default values if not configured
	if minTime == 0 {
		minTime = 50 * time.Millisecond
	}
	if maxTime == 0 {
		maxTime = 200 * time.Millisecond
	}

	// For slow processing mode, use much longer times to trigger rebalances
	if c.config.EnableSlowProcessing {
		minTime = c.config.SessionTimeout + 1*time.Second
		maxTime = c.config.SessionTimeout + 3*time.Second
	}

	// Random time between min and max
	if maxTime <= minTime {
		return minTime
	}

	// Simple randomization using current time
	jitter := time.Duration(time.Now().UnixNano()%(maxTime-minTime).Nanoseconds() + minTime.Nanoseconds())
	return jitter
}
