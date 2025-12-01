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
	PartitionID         int32
	MessagesReceived    int
	ExpectedSequence    int
	OrderingViolations  []OrderingViolation
	DuplicateMessages   []DuplicateMessage
	Gaps                []Gap
	FirstMessage        *ConsumedMessage
	LastMessage         *ConsumedMessage
	SeenSequences       map[int]int // sequence -> count
	mu                  sync.Mutex
}

// OrderingViolation represents a true out-of-order message (sequence went backwards)
type OrderingViolation struct {
	Expected       int
	Received       int
	Offset         int64
	Timestamp      time.Time
	ConsumerID     string
	MessageID      string
}

// DuplicateMessage represents a message that was processed multiple times
type DuplicateMessage struct {
	SequenceNum    int
	MessageID      string
	Offset         int64
	Timestamp      time.Time
	ConsumerID     string
	ProcessCount   int // How many times this sequence was seen
}

// Gap represents a gap in message sequence
type Gap struct {
	StartSequence int
	EndSequence   int
	Timestamp     time.Time
}

// ProcessingState represents the state of message processing
type ProcessingState string

const (
	ProcessingStarted     ProcessingState = "STARTED"
	ProcessingCompleted   ProcessingState = "COMPLETED"
	ProcessingInterrupted ProcessingState = "INTERRUPTED"
)

// MessageProcessingInfo tracks the processing lifecycle of a message
type MessageProcessingInfo struct {
	MessageID         string
	Partition         int32
	Offset            int64
	SequenceNum       int
	ConsumerID        string
	State             ProcessingState
	ProcessingStarted time.Time
	ProcessingEnded   time.Time
	ProcessingDuration time.Duration
	Interrupted       bool
	ReprocessedBy     string    // Consumer ID that reprocessed after interruption
	ReprocessedAt     time.Time
	ProcessingAttempts int
	InterruptedDuringRebalance bool
}

// ProcessingStats tracks message processing statistics
type ProcessingStats struct {
	TotalProcessed           int
	TotalInterrupted         int
	TotalReprocessed         int
	InterruptedMessages      []*MessageProcessingInfo
	ReprocessedMessages      []*MessageProcessingInfo
	DuplicateProcessing      []*MessageProcessingInfo
	AverageProcessingTime    time.Duration
	AverageReprocessingDelay time.Duration
	mu                       sync.RWMutex
}

// TestConfig holds the test configuration
type TestConfig struct {
	TopicName              string
	NumPartitions          int
	NumMessages            int
	MessageDelay           time.Duration
	ConsumerGroup          string
	KafkaBrokers           []string
	ProcessingTimeMin      time.Duration // Minimum processing time per message
	ProcessingTimeMax      time.Duration // Maximum processing time per message
	SessionTimeout         time.Duration // Consumer session timeout
	HeartbeatInterval      time.Duration // Consumer heartbeat interval
	EnableSlowProcessing   bool          // Enable slow processing mode to trigger rebalances
}

// TestResults aggregates all test results
type TestResults struct {
	Config            TestConfig
	PartitionStats    map[int32]*PartitionStats
	RebalanceEvents   []RebalanceEvent
	ProcessingStats   *ProcessingStats
	ProcessingInfo    map[string]*MessageProcessingInfo // Key: MessageID
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
		ProcessingStats: &ProcessingStats{
			InterruptedMessages: make([]*MessageProcessingInfo, 0),
			ReprocessedMessages: make([]*MessageProcessingInfo, 0),
			DuplicateProcessing: make([]*MessageProcessingInfo, 0),
		},
		ProcessingInfo: make(map[string]*MessageProcessingInfo),
		StartTime:      time.Now(),
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
			DuplicateMessages:  make([]DuplicateMessage, 0),
			Gaps:               make([]Gap, 0),
			SeenSequences:      make(map[int]int),
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

// StartProcessing marks a message as started processing
func (tr *TestResults) StartProcessing(msgID string, partition int32, offset int64, seqNum int, consumerID string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if existing, exists := tr.ProcessingInfo[msgID]; exists {
		// This is a reprocessing attempt
		existing.ProcessingAttempts++
		existing.ReprocessedBy = consumerID
		existing.ReprocessedAt = time.Now()
		existing.State = ProcessingStarted
		existing.ProcessingStarted = time.Now()
	} else {
		// First time processing
		tr.ProcessingInfo[msgID] = &MessageProcessingInfo{
			MessageID:         msgID,
			Partition:         partition,
			Offset:            offset,
			SequenceNum:       seqNum,
			ConsumerID:        consumerID,
			State:             ProcessingStarted,
			ProcessingStarted: time.Now(),
			ProcessingAttempts: 1,
		}
	}
}

// CompleteProcessing marks a message as completed processing
func (tr *TestResults) CompleteProcessing(msgID string, consumerID string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if info, exists := tr.ProcessingInfo[msgID]; exists {
		info.State = ProcessingCompleted
		info.ProcessingEnded = time.Now()
		info.ProcessingDuration = info.ProcessingEnded.Sub(info.ProcessingStarted)

		// Update stats
		tr.ProcessingStats.mu.Lock()
		tr.ProcessingStats.TotalProcessed++

		if info.Interrupted {
			tr.ProcessingStats.TotalReprocessed++
			tr.ProcessingStats.ReprocessedMessages = append(tr.ProcessingStats.ReprocessedMessages, info)

			// Calculate reprocessing delay
			if !info.ReprocessedAt.IsZero() && !info.ProcessingStarted.IsZero() {
				delay := info.ReprocessedAt.Sub(info.ProcessingStarted)
				if tr.ProcessingStats.AverageReprocessingDelay == 0 {
					tr.ProcessingStats.AverageReprocessingDelay = delay
				} else {
					// Running average
					count := len(tr.ProcessingStats.ReprocessedMessages)
					tr.ProcessingStats.AverageReprocessingDelay =
						(tr.ProcessingStats.AverageReprocessingDelay*time.Duration(count-1) + delay) / time.Duration(count)
				}
			}
		}

		if info.ProcessingAttempts > 1 {
			tr.ProcessingStats.DuplicateProcessing = append(tr.ProcessingStats.DuplicateProcessing, info)
		}

		// Update average processing time
		if tr.ProcessingStats.AverageProcessingTime == 0 {
			tr.ProcessingStats.AverageProcessingTime = info.ProcessingDuration
		} else {
			count := tr.ProcessingStats.TotalProcessed
			tr.ProcessingStats.AverageProcessingTime =
				(tr.ProcessingStats.AverageProcessingTime*time.Duration(count-1) + info.ProcessingDuration) / time.Duration(count)
		}

		tr.ProcessingStats.mu.Unlock()
	}
}

// InterruptProcessing marks a message as interrupted during rebalance
func (tr *TestResults) InterruptProcessing(msgID string) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if info, exists := tr.ProcessingInfo[msgID]; exists {
		if info.State == ProcessingStarted {
			info.State = ProcessingInterrupted
			info.Interrupted = true
			info.InterruptedDuringRebalance = true

			tr.ProcessingStats.mu.Lock()
			tr.ProcessingStats.TotalInterrupted++
			tr.ProcessingStats.InterruptedMessages = append(tr.ProcessingStats.InterruptedMessages, info)
			tr.ProcessingStats.mu.Unlock()
		}
	}
}

// GetInFlightMessages returns all messages currently being processed
func (tr *TestResults) GetInFlightMessages() []*MessageProcessingInfo {
	tr.mu.RLock()
	defer tr.mu.RUnlock()

	inFlight := make([]*MessageProcessingInfo, 0)
	for _, info := range tr.ProcessingInfo {
		if info.State == ProcessingStarted {
			inFlight = append(inFlight, info)
		}
	}
	return inFlight
}
