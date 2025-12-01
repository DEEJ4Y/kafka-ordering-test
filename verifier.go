package main

import (
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

	// Check ordering
	receivedSeq := msg.Message.SequenceNum
	expectedSeq := stats.ExpectedSequence

	if receivedSeq == expectedSeq {
		// Perfect order
		stats.ExpectedSequence++
		v.logger.Printf("[VERIFY] Partition %d: In order - Seq=%d, Offset=%d, Consumer=%s",
			msg.Partition, receivedSeq, msg.Offset, msg.ConsumerID)
	} else if receivedSeq > expectedSeq {
		// Gap detected
		gap := Gap{
			StartSequence: expectedSeq,
			EndSequence:   receivedSeq - 1,
			Timestamp:     msg.ConsumedAt,
		}
		stats.Gaps = append(stats.Gaps, gap)
		stats.ExpectedSequence = receivedSeq + 1

		v.logger.Printf("[VERIFY] Partition %d: GAP DETECTED - Expected=%d, Received=%d, Gap=[%d-%d], Offset=%d, Consumer=%s",
			msg.Partition, expectedSeq, receivedSeq, gap.StartSequence, gap.EndSequence, msg.Offset, msg.ConsumerID)
	} else {
		// Out of order or duplicate
		if receivedSeq < expectedSeq {
			// Check if it's a duplicate
			isDuplicate := false
			for _, dupSeq := range stats.Duplicates {
				if dupSeq == receivedSeq {
					isDuplicate = true
					break
				}
			}

			if !isDuplicate {
				// This is an ordering violation (message came after we expected it)
				violation := OrderingViolation{
					Expected:   expectedSeq,
					Received:   receivedSeq,
					Offset:     msg.Offset,
					Timestamp:  msg.ConsumedAt,
					ConsumerID: msg.ConsumerID,
				}
				stats.OrderingViolations = append(stats.OrderingViolations, violation)

				v.logger.Printf("[VERIFY] Partition %d: ORDERING VIOLATION - Expected=%d, Received=%d, Offset=%d, Consumer=%s",
					msg.Partition, expectedSeq, receivedSeq, msg.Offset, msg.ConsumerID)
			} else {
				v.logger.Printf("[VERIFY] Partition %d: DUPLICATE - Seq=%d (already processed), Offset=%d, Consumer=%s",
					msg.Partition, receivedSeq, msg.Offset, msg.ConsumerID)
			}

			// Track duplicate
			stats.Duplicates = append(stats.Duplicates, receivedSeq)
		}
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
	v.logger.Printf("Total messages received: %d", totalReceived)
	v.logger.Printf("Ordering preserved: %v", v.results.OrderingPreserved)

	for partitionID, stats := range v.results.PartitionStats {
		stats.mu.Lock()
		v.logger.Printf("Partition %d: %d messages, %d violations, %d gaps, %d duplicates",
			partitionID, stats.MessagesReceived, len(stats.OrderingViolations),
			len(stats.Gaps), len(stats.Duplicates))
		stats.mu.Unlock()
	}
}
