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
		TopicName:     topicName,
		NumPartitions: numPartitions,
		NumMessages:   numMessages,
		MessageDelay:  10 * time.Millisecond,
		ConsumerGroup: consumerGroup,
		KafkaBrokers:  []string{"localhost:9092"},
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

	// Phase 1: Start 2 consumers
	logger.Println("\n=== PHASE 1: Starting 2 consumers ===")
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
		time.Sleep(1 * time.Second) // Stagger consumer starts
	}

	// Wait for consumers to be ready
	time.Sleep(3 * time.Second)

	// Phase 2: Start producer
	logger.Println("\n=== PHASE 2: Starting message production ===")
	producer, err := NewProducer(config, logger)
	if err != nil {
		logger.Fatalf("Failed to create producer: %v", err)
	}

	// Start producing in background
	producerDone := make(chan bool)
	go func() {
		if err := producer.ProduceMessages(); err != nil {
			logger.Printf("Producer error: %v", err)
		}
		results.TotalMessagesSent = numMessages
		producerDone <- true
	}()

	// Wait a bit for some messages to be produced
	time.Sleep(2 * time.Second)

	// Phase 3: Add 3rd consumer (trigger rebalance)
	logger.Println("\n=== PHASE 3: Adding 3rd consumer (triggering rebalance) ===")
	consumerID := "consumer-2"
	consumer, err := NewConsumer(consumerID, config, logger, results, verifier)
	if err != nil {
		logger.Fatalf("Failed to create consumer %s: %v", consumerID, err)
	}
	consumers = append(consumers, consumer)

	wg.Add(1)
	go consumer.Start(&wg)
	time.Sleep(3 * time.Second)

	// Phase 4: Add 4th consumer (more consumers than partitions)
	logger.Println("\n=== PHASE 4: Adding 4th consumer (more consumers than partitions) ===")
	consumerID = "consumer-3"
	consumer, err = NewConsumer(consumerID, config, logger, results, verifier)
	if err != nil {
		logger.Fatalf("Failed to create consumer %s: %v", consumerID, err)
	}
	consumers = append(consumers, consumer)

	wg.Add(1)
	go consumer.Start(&wg)
	time.Sleep(3 * time.Second)

	// Wait for producer to finish
	logger.Println("\n=== Waiting for producer to finish ===")
	<-producerDone
	producer.Close()
	logger.Println("Producer finished!")

	// Phase 5: Let consumers catch up
	logger.Println("\n=== PHASE 5: Letting consumers catch up ===")
	time.Sleep(5 * time.Second)

	// Phase 6: Remove consumers (trigger rebalances)
	logger.Println("\n=== PHASE 6: Removing consumers (triggering rebalances) ===")

	// Remove consumer 3
	logger.Println("Removing consumer-3...")
	consumers[3].Stop()
	time.Sleep(3 * time.Second)

	// Remove consumer 2
	logger.Println("Removing consumer-2...")
	consumers[2].Stop()
	time.Sleep(3 * time.Second)

	// Let remaining consumers process
	logger.Println("\n=== Waiting for final message processing ===")
	time.Sleep(5 * time.Second)

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
