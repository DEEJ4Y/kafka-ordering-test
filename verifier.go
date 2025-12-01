package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// OrderVerifier verifies message ordering
type OrderVerifier struct {
	results *TestResults
	logger  *log.Logger
	mu      sync.Mutex
}

// NewOrderVerifier creates a new OrderVerifier
func NewOrderVerifier(results *TestResults, logger *log.Logger) *OrderVerifier {
	return &OrderVerifier{
		results: results,
		logger:  logger,
	}
}

// VerifyMessage verifies a consumed message for ordering
func (v *OrderVerifier) VerifyMessage(msg ConsumedMessage) {
	v.mu.Lock()
	defer v.mu.Unlock()

	stats := v.results.GetPartitionStats(msg.Partition)
	stats.mu.Lock()
	defer stats.mu.Unlock()

	// Increment message count
	stats.MessagesReceived++

	// Set first message if this is the first one
	if stats.FirstMessage == nil {
		stats.FirstMessage = &msg
	}

	// Always update last message
	stats.LastMessage = &msg

	receivedSeq := msg.Message.SequenceNum
	expectedSeq := stats.ExpectedSequence

	// Track how many times we've seen this sequence
	stats.SeenSequences[receivedSeq]++
	seenCount := stats.SeenSequences[receivedSeq]

	// Check if this is a duplicate (we've seen this sequence before)
	if seenCount > 1 {
		// This is a duplicate message (reprocessed due to rebalance or commit timing)
		duplicate := DuplicateMessage{
			SequenceNum:  receivedSeq,
			MessageID:    msg.Message.MessageID,
			Offset:       msg.Offset,
			Timestamp:    msg.ConsumedAt,
			ConsumerID:   msg.ConsumerID,
			ProcessCount: seenCount,
		}
		stats.DuplicateMessages = append(stats.DuplicateMessages, duplicate)

		v.logger.Printf("[VERIFY] Partition %d: DUPLICATE (reprocessed) - Seq=%d, ProcessCount=%d, MsgID=%s, Offset=%d, Consumer=%s",
			msg.Partition, receivedSeq, seenCount, msg.Message.MessageID, msg.Offset, msg.ConsumerID)

		// Duplicates don't affect ordering verification
		return
	}

	// Now check ordering (only for first occurrence of each sequence)
	if receivedSeq == expectedSeq {
		// Perfect order - next message in sequence
		stats.ExpectedSequence++
		v.logger.Printf("[VERIFY] Partition %d: ✓ In order - Seq=%d, MsgID=%s, Offset=%d, Consumer=%s",
			msg.Partition, receivedSeq, msg.Message.MessageID, msg.Offset, msg.ConsumerID)

	} else if receivedSeq > expectedSeq {
		// Gap detected - some messages were skipped (may arrive later)
		gap := Gap{
			StartSequence: expectedSeq,
			EndSequence:   receivedSeq - 1,
			Timestamp:     msg.ConsumedAt,
		}
		stats.Gaps = append(stats.Gaps, gap)
		stats.ExpectedSequence = receivedSeq + 1

		v.logger.Printf("[VERIFY] Partition %d: ⚠️  GAP - Expected=%d, Received=%d, Gap=[%d-%d], MsgID=%s, Offset=%d, Consumer=%s",
			msg.Partition, expectedSeq, receivedSeq, gap.StartSequence, gap.EndSequence, msg.Message.MessageID, msg.Offset, msg.ConsumerID)

	} else {
		// receivedSeq < expectedSeq
		// This is a TRUE ORDERING VIOLATION - sequence went backwards!
		// This should NEVER happen with Kafka's ordering guarantees
		// (unless there's a bug in the producer or Kafka itself)
		violation := OrderingViolation{
			Expected:   expectedSeq,
			Received:   receivedSeq,
			Offset:     msg.Offset,
			Timestamp:  msg.ConsumedAt,
			ConsumerID: msg.ConsumerID,
			MessageID:  msg.Message.MessageID,
		}
		stats.OrderingViolations = append(stats.OrderingViolations, violation)

		v.logger.Printf("[VERIFY] Partition %d: ❌ ORDERING VIOLATION! - Expected=%d, Received=%d, MsgID=%s, Offset=%d, Consumer=%s",
			msg.Partition, expectedSeq, receivedSeq, msg.Message.MessageID, msg.Offset, msg.ConsumerID)
		v.logger.Printf("         This indicates sequence went BACKWARDS which should NEVER happen!")
	}
}

// FinalizeResults performs final analysis of the test results
func (v *OrderVerifier) FinalizeResults() {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.results.EndTime = time.Now()
	v.results.OrderingPreserved = true

	// Calculate total messages received and check ordering
	totalReceived := 0
	for _, stats := range v.results.PartitionStats {
		stats.mu.Lock()
		totalReceived += stats.MessagesReceived

		// If there are any ordering violations, ordering is not preserved
		if len(stats.OrderingViolations) > 0 {
			v.results.OrderingPreserved = false
		}
		stats.mu.Unlock()
	}

	v.results.TotalMessagesRecv = totalReceived

	v.logger.Printf("=== VERIFICATION SUMMARY ===")
	v.logger.Printf("Total messages received (including duplicates): %d", totalReceived)
	v.logger.Printf("Total unique messages received: %d", len(uniqueMessages(v.results)))
	v.logger.Printf("Ordering preserved within partitions: %v", v.results.OrderingPreserved)
	v.logger.Printf("")

	for partitionID, stats := range v.results.PartitionStats {
		stats.mu.Lock()
		uniqueSeqs := len(stats.SeenSequences)
		v.logger.Printf("Partition %d:", partitionID)
		v.logger.Printf("  Total messages received: %d", stats.MessagesReceived)
		v.logger.Printf("  Unique sequences: %d", uniqueSeqs)
		v.logger.Printf("  Duplicate messages: %d", len(stats.DuplicateMessages))
		v.logger.Printf("  Ordering violations (sequences went backwards): %d", len(stats.OrderingViolations))
		v.logger.Printf("  Gaps (sequences jumped forward): %d", len(stats.Gaps))
		stats.mu.Unlock()
	}
}

// uniqueMessages counts unique message IDs across all partitions
func uniqueMessages(results *TestResults) map[string]bool {
	unique := make(map[string]bool)
	for _, stats := range results.PartitionStats {
		stats.mu.Lock()
		for seq := range stats.SeenSequences {
			// Reconstruct message ID from partition and sequence
			msgID := fmt.Sprintf("key-%d-seq-%d", stats.PartitionID, seq)
			unique[msgID] = true
		}
		stats.mu.Unlock()
	}
	return unique
}
