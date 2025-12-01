package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	topicName     = "ordering-test"
	numPartitions = 3
	numMessages   = 600
	consumerGroup = "ordering-test-group"
)

func main() {
	// Create logger
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	logger.Println("=== Kafka Ordering Test Bench ===")
	logger.Println("This test demonstrates that Kafka preserves message ordering within partitions")
	logger.Println("but not across partitions, even during consumer rebalancing.")
	logger.Println()

	// Test configuration
	config := TestConfig{
		TopicName:          topicName,
		NumPartitions:      numPartitions,
		NumMessages:        numMessages,
		MessageDelay:       10 * time.Millisecond,
		ConsumerGroup:      consumerGroup,
		KafkaBrokers:       []string{"localhost:9092"},
		ProcessingTimeMin:  50 * time.Millisecond,  // Minimum processing time
		ProcessingTimeMax:  200 * time.Millisecond, // Maximum processing time
		SessionTimeout:     10 * time.Second,        // Consumer session timeout
		HeartbeatInterval:  3 * time.Second,         // Heartbeat interval
		EnableSlowProcessing: false,                 // Set to true to test slow processing causing rebalances
	}

	// Initialize test results
	results := NewTestResults(config)

	// Wait for Kafka to be ready
	logger.Println("Waiting for Kafka to be ready...")
	if err := waitForKafka(config.KafkaBrokers, 30*time.Second); err != nil {
		logger.Fatalf("Kafka is not ready: %v", err)
	}
	logger.Println("Kafka is ready!")

	// Create topic with 3 partitions
	logger.Printf("Creating topic '%s' with %d partitions...", topicName, numPartitions)
	if err := createTopic(config); err != nil {
		logger.Printf("Warning: Failed to create topic (may already exist): %v", err)
	} else {
		logger.Println("Topic created successfully!")
	}

	// Wait a bit for topic to be ready
	time.Sleep(2 * time.Second)

	// Create verifier
	verifier := NewOrderVerifier(results, logger)

	// ========================================================================
	// PHASE 1: Produce ALL messages FIRST (before any consumers)
	// ========================================================================
	logger.Println("\n=== PHASE 1: Producing all messages (consumers not started yet) ===")
	producer, err := NewProducer(config, logger)
	if err != nil {
		logger.Fatalf("Failed to create producer: %v", err)
	}

	messagesSent, err := producer.ProduceMessages()
	if err != nil {
		logger.Printf("WARNING: Producer encountered errors: %v", err)
	}
	producer.Close()

	results.TotalMessagesSent = messagesSent
	logger.Printf("✓ Producer complete! Successfully sent %d/%d messages\n", messagesSent, numMessages)

	// Wait for messages to be fully written to Kafka
	time.Sleep(2 * time.Second)

	// ========================================================================
	// PHASE 2: Start 2 consumers and let them begin processing
	// ========================================================================
	logger.Println("\n=== PHASE 2: Starting 2 consumers ===")
	consumers := make([]*Consumer, 0)
	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		consumerID := fmt.Sprintf("consumer-%d", i)
		consumer, err := NewConsumer(consumerID, config, logger, results, verifier)
		if err != nil {
			logger.Fatalf("Failed to create consumer %s: %v", consumerID, err)
		}
		consumers = append(consumers, consumer)

		wg.Add(1)
		go consumer.Start(&wg)
		time.Sleep(1 * time.Second)
	}

	// Wait for consumers to be ready and start processing
	time.Sleep(3 * time.Second)

	// ========================================================================
	// PHASE 3: Add 3rd consumer after ~25% of messages processed
	// ========================================================================
	logger.Println("\n=== PHASE 3: Letting consumers process (~25% of messages) ===")
	// With 600 messages and 50-200ms processing, expect ~10-40 seconds total
	// Wait for ~25% = 2-10 seconds
	time.Sleep(5 * time.Second)

	logger.Println("\n=== PHASE 3b: Adding 3rd consumer (triggering rebalance) ===")
	consumerID := "consumer-2"
	consumer, err := NewConsumer(consumerID, config, logger, results, verifier)
	if err != nil {
		logger.Fatalf("Failed to create consumer %s: %v", consumerID, err)
	}
	consumers = append(consumers, consumer)

	wg.Add(1)
	go consumer.Start(&wg)
	time.Sleep(3 * time.Second)

	// ========================================================================
	// PHASE 4: Add 4th consumer after ~50% of messages processed
	// ========================================================================
	logger.Println("\n=== PHASE 4: Letting consumers process more (~50% total) ===")
	time.Sleep(5 * time.Second)

	logger.Println("\n=== PHASE 4b: Adding 4th consumer (more consumers than partitions) ===")
	consumerID = "consumer-3"
	consumer, err = NewConsumer(consumerID, config, logger, results, verifier)
	if err != nil {
		logger.Fatalf("Failed to create consumer %s: %v", consumerID, err)
	}
	consumers = append(consumers, consumer)

	wg.Add(1)
	go consumer.Start(&wg)
	time.Sleep(3 * time.Second)

	// ========================================================================
	// PHASE 5: Let all consumers process remaining messages
	// ========================================================================
	logger.Println("\n=== PHASE 5: Letting all consumers process remaining messages ===")
	// With 600 messages, 3 partitions, 4 consumers (one idle), ~200 msgs per active consumer
	// At 50-200ms per message: 10-40 seconds worst case
	// Wait generously to ensure all messages are processed
	logger.Println("Waiting 30 seconds for complete processing...")
	time.Sleep(30 * time.Second)

	// ========================================================================
	// PHASE 6: Remove consumers (trigger rebalances) while there's little/no work
	// ========================================================================
	logger.Println("\n=== PHASE 6: Removing consumers (triggering rebalances) ===")

	// Remove consumer 3
	logger.Println("Removing consumer-3...")
	consumers[3].Stop()
	time.Sleep(3 * time.Second)

	// Remove consumer 2
	logger.Println("Removing consumer-2...")
	consumers[2].Stop()
	time.Sleep(3 * time.Second)

	// ========================================================================
	// PHASE 7: Final processing and verification
	// ========================================================================
	logger.Println("\n=== PHASE 7: Final processing by remaining consumers ===")
	// Give final consumers time to pick up any remaining uncommitted messages
	time.Sleep(10 * time.Second)

	// Stop remaining consumers
	logger.Println("\n=== Stopping all consumers ===")
	for i := 0; i < 2; i++ {
		consumers[i].Stop()
	}

	// Wait for all consumer goroutines to finish
	wg.Wait()

	// Finalize results
	logger.Println("\n=== Finalizing results ===")
	verifier.FinalizeResults()

	// Generate reports
	logger.Println("\n=== Generating reports ===")
	reporter := NewReporter(results)

	if err := reporter.GenerateMarkdownReport("RESULTS.md"); err != nil {
		logger.Printf("Error generating markdown report: %v", err)
	} else {
		logger.Println("✓ Generated RESULTS.md")
	}

	if err := reporter.GenerateRawLog("test-events.log"); err != nil {
		logger.Printf("Error generating raw log: %v", err)
	} else {
		logger.Println("✓ Generated test-events.log")
	}

	// Print final summary
	logger.Println("\n" + strings.Repeat("=", 70))
	logger.Println("=== TEST COMPLETE ===")
	logger.Println(strings.Repeat("=", 70))
	logger.Printf("Messages sent: %d", results.TotalMessagesSent)
	logger.Printf("Messages received: %d", results.TotalMessagesRecv)
	logger.Printf("Rebalance events: %d", len(results.RebalanceEvents))
	logger.Printf("Ordering preserved within partitions: %v", results.OrderingPreserved)
	logger.Println(strings.Repeat("-", 70))
	logger.Printf("Messages processed: %d", results.ProcessingStats.TotalProcessed)
	logger.Printf("Messages interrupted during rebalance: %d", results.ProcessingStats.TotalInterrupted)
	logger.Printf("Messages reprocessed: %d", results.ProcessingStats.TotalReprocessed)
	logger.Printf("Average processing time: %s", results.ProcessingStats.AverageProcessingTime.Round(time.Millisecond))
	if results.ProcessingStats.AverageReprocessingDelay > 0 {
		logger.Printf("Average reprocessing delay: %s", results.ProcessingStats.AverageReprocessingDelay.Round(time.Millisecond))
	}
	logger.Println(strings.Repeat("=", 70))
	logger.Println("\nCheck RESULTS.md for detailed analysis!")
}

// waitForKafka waits for Kafka to be ready
func waitForKafka(brokers []string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for Kafka")
		case <-ticker.C:
			config := sarama.NewConfig()
			config.Version = sarama.V2_6_0_0

			client, err := sarama.NewClient(brokers, config)
			if err == nil {
				client.Close()
				return nil
			}
		}
	}
}

// createTopic creates a Kafka topic with specified partitions
func createTopic(config TestConfig) error {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_6_0_0

	admin, err := sarama.NewClusterAdmin(config.KafkaBrokers, saramaConfig)
	if err != nil {
		return fmt.Errorf("failed to create cluster admin: %w", err)
	}
	defer admin.Close()

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(config.NumPartitions),
		ReplicationFactor: 1,
	}

	err = admin.CreateTopic(config.TopicName, topicDetail, false)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}
