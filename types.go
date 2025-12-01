package main

import (
	"sync"
	"time"
)

// TestMessage represents a message sent to Kafka
type TestMessage struct {
	PartitionKey string `json:"partition_key"` // e.g., "key-0", "key-1", "key-2"
	SequenceNum  int    `json:"sequence_num"`  // Sequence number within this partition
	Timestamp    int64  `json:"timestamp"`     // Unix timestamp when message was sent
	MessageID    string `json:"message_id"`    // Unique message identifier
}

// ConsumedMessage represents a message consumed from Kafka with metadata
type ConsumedMessage struct {
	Message      TestMessage
	ConsumerID   string
	Partition    int32
	Offset       int64
	ConsumedAt   time.Time
}

// RebalanceEvent represents a consumer group rebalance event
type RebalanceEvent struct {
	Timestamp      time.Time
	EventType      string // "ASSIGN", "REVOKE"
	ConsumerID     string
	Partitions     []int32
	PartitionCount int
}

// PartitionStats tracks statistics for a single partition
type PartitionStats struct {
	PartitionID       int32
	MessagesReceived  int
	ExpectedSequence  int
	OrderingViolations []OrderingViolation
	Duplicates        []int
	Gaps              []Gap
	FirstMessage      *ConsumedMessage
	LastMessage       *ConsumedMessage
	mu                sync.Mutex
}

// OrderingViolation represents an out-of-order message
type OrderingViolation struct {
	Expected       int
	Received       int
	Offset         int64
	Timestamp      time.Time
	ConsumerID     string
}

// Gap represents a gap in message sequence
type Gap struct {
	StartSequence int
	EndSequence   int
	Timestamp     time.Time
}

// TestConfig holds the test configuration
type TestConfig struct {
	TopicName        string
	NumPartitions    int
	NumMessages      int
	MessageDelay     time.Duration
	ConsumerGroup    string
	KafkaBrokers     []string
}

// TestResults aggregates all test results
type TestResults struct {
	Config            TestConfig
	PartitionStats    map[int32]*PartitionStats
	RebalanceEvents   []RebalanceEvent
	StartTime         time.Time
	EndTime           time.Time
	TotalMessagesSent int
	TotalMessagesRecv int
	OrderingPreserved bool
	mu                sync.RWMutex
}

// NewTestResults creates a new TestResults instance
func NewTestResults(config TestConfig) *TestResults {
	return &TestResults{
		Config:          config,
		PartitionStats:  make(map[int32]*PartitionStats),
		RebalanceEvents: make([]RebalanceEvent, 0),
		StartTime:       time.Now(),
	}
}

// GetPartitionStats returns stats for a partition (creates if not exists)
func (tr *TestResults) GetPartitionStats(partition int32) *PartitionStats {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if _, exists := tr.PartitionStats[partition]; !exists {
		tr.PartitionStats[partition] = &PartitionStats{
			PartitionID:        partition,
			OrderingViolations: make([]OrderingViolation, 0),
			Duplicates:         make([]int, 0),
			Gaps:               make([]Gap, 0),
		}
	}
	return tr.PartitionStats[partition]
}

// AddRebalanceEvent adds a rebalance event to the results
func (tr *TestResults) AddRebalanceEvent(event RebalanceEvent) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	tr.RebalanceEvents = append(tr.RebalanceEvents, event)
}
