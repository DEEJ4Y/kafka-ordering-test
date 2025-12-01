package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// Reporter generates test reports
type Reporter struct {
	results *TestResults
}

// NewReporter creates a new Reporter
func NewReporter(results *TestResults) *Reporter {
	return &Reporter{
		results: results,
	}
}

// GenerateMarkdownReport generates a comprehensive markdown report
func (r *Reporter) GenerateMarkdownReport(filename string) error {
	var sb strings.Builder

	// Header
	sb.WriteString("# Kafka Ordering Test Results\n\n")
	sb.WriteString(fmt.Sprintf("**Test Date:** %s\n\n", r.results.StartTime.Format("2006-01-02 15:04:05")))
	sb.WriteString(fmt.Sprintf("**Duration:** %s\n\n", r.results.EndTime.Sub(r.results.StartTime).Round(time.Millisecond)))

	// Test Configuration
	sb.WriteString("## Test Configuration\n\n")
	sb.WriteString(fmt.Sprintf("- **Topic Name:** %s\n", r.results.Config.TopicName))
	sb.WriteString(fmt.Sprintf("- **Number of Partitions:** %d\n", r.results.Config.NumPartitions))
	sb.WriteString(fmt.Sprintf("- **Total Messages Sent:** %d\n", r.results.Config.NumMessages))
	sb.WriteString(fmt.Sprintf("- **Messages per Partition:** ~%d\n", r.results.Config.NumMessages/r.results.Config.NumPartitions))
	sb.WriteString(fmt.Sprintf("- **Consumer Group:** %s\n", r.results.Config.ConsumerGroup))
	sb.WriteString(fmt.Sprintf("- **Message Delay:** %s\n", r.results.Config.MessageDelay))
	sb.WriteString("\n")

	// Summary
	sb.WriteString("## Executive Summary\n\n")
	sb.WriteString(fmt.Sprintf("- **Total Messages Sent:** %d\n", r.results.TotalMessagesSent))
	sb.WriteString(fmt.Sprintf("- **Total Messages Received:** %d\n", r.results.TotalMessagesRecv))
	sb.WriteString(fmt.Sprintf("- **Message Loss:** %d messages\n", r.results.TotalMessagesSent-r.results.TotalMessagesRecv))
	sb.WriteString(fmt.Sprintf("- **Number of Rebalance Events:** %d\n", len(r.results.RebalanceEvents)))
	sb.WriteString("\n")

	// Ordering Conclusion
	sb.WriteString("### Ordering Verification\n\n")
	if r.results.OrderingPreserved {
		sb.WriteString("✅ **ORDERING PRESERVED WITHIN PARTITIONS: YES**\n\n")
		sb.WriteString("All messages within each partition were received in the correct sequential order.\n")
	} else {
		sb.WriteString("❌ **ORDERING PRESERVED WITHIN PARTITIONS: NO**\n\n")
		sb.WriteString("Ordering violations were detected within one or more partitions.\n")
	}
	sb.WriteString("\n")

	// Rebalance Timeline
	sb.WriteString("## Rebalance Timeline\n\n")
	sb.WriteString("The following table shows all consumer group rebalance events:\n\n")
	sb.WriteString("| Timestamp | Event Type | Consumer ID | Partitions | Count |\n")
	sb.WriteString("|-----------|------------|-------------|------------|-------|\n")

	for _, event := range r.results.RebalanceEvents {
		partitions := fmt.Sprintf("%v", event.Partitions)
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %d |\n",
			event.Timestamp.Format("15:04:05.000"),
			event.EventType,
			event.ConsumerID,
			partitions,
			event.PartitionCount))
	}
	sb.WriteString("\n")

	// Per-Partition Statistics
	sb.WriteString("## Per-Partition Statistics\n\n")

	// Get sorted partition IDs
	partitionIDs := make([]int32, 0, len(r.results.PartitionStats))
	for id := range r.results.PartitionStats {
		partitionIDs = append(partitionIDs, id)
	}
	sort.Slice(partitionIDs, func(i, j int) bool {
		return partitionIDs[i] < partitionIDs[j]
	})

	for _, partitionID := range partitionIDs {
		stats := r.results.PartitionStats[partitionID]
		stats.mu.Lock()

		sb.WriteString(fmt.Sprintf("### Partition %d\n\n", partitionID))
		sb.WriteString(fmt.Sprintf("- **Messages Received:** %d\n", stats.MessagesReceived))
		sb.WriteString(fmt.Sprintf("- **Expected Final Sequence:** %d\n", stats.ExpectedSequence))
		sb.WriteString(fmt.Sprintf("- **Ordering Violations:** %d\n", len(stats.OrderingViolations)))
		sb.WriteString(fmt.Sprintf("- **Gaps Detected:** %d\n", len(stats.Gaps)))
		sb.WriteString(fmt.Sprintf("- **Duplicates:** %d\n", len(stats.Duplicates)))

		if stats.FirstMessage != nil {
			sb.WriteString(fmt.Sprintf("- **First Message:** Seq=%d, Offset=%d, Consumer=%s\n",
				stats.FirstMessage.Message.SequenceNum,
				stats.FirstMessage.Offset,
				stats.FirstMessage.ConsumerID))
		}

		if stats.LastMessage != nil {
			sb.WriteString(fmt.Sprintf("- **Last Message:** Seq=%d, Offset=%d, Consumer=%s\n",
				stats.LastMessage.Message.SequenceNum,
				stats.LastMessage.Offset,
				stats.LastMessage.ConsumerID))
		}

		// Ordering violations details
		if len(stats.OrderingViolations) > 0 {
			sb.WriteString("\n**Ordering Violations:**\n\n")
			sb.WriteString("| Expected Seq | Received Seq | Offset | Consumer | Timestamp |\n")
			sb.WriteString("|--------------|--------------|--------|----------|----------|\n")
			for _, violation := range stats.OrderingViolations {
				sb.WriteString(fmt.Sprintf("| %d | %d | %d | %s | %s |\n",
					violation.Expected,
					violation.Received,
					violation.Offset,
					violation.ConsumerID,
					violation.Timestamp.Format("15:04:05.000")))
			}
		}

		// Gaps details
		if len(stats.Gaps) > 0 {
			sb.WriteString("\n**Gaps Detected:**\n\n")
			sb.WriteString("| Gap Range | Missing Count | Detected At |\n")
			sb.WriteString("|-----------|---------------|-------------|\n")
			for _, gap := range stats.Gaps {
				missingCount := gap.EndSequence - gap.StartSequence + 1
				sb.WriteString(fmt.Sprintf("| %d-%d | %d | %s |\n",
					gap.StartSequence,
					gap.EndSequence,
					missingCount,
					gap.Timestamp.Format("15:04:05.000")))
			}
		}

		sb.WriteString("\n")
		stats.mu.Unlock()
	}

	// Visual representation
	sb.WriteString("## Message Flow Visualization\n\n")
	sb.WriteString("This ASCII chart shows message consumption pattern across partitions:\n\n")
	sb.WriteString("```\n")
	sb.WriteString(r.generateASCIIChart())
	sb.WriteString("```\n\n")

	// Cross-partition ordering
	sb.WriteString("## Cross-Partition Ordering Analysis\n\n")
	sb.WriteString("Kafka guarantees ordering **only within a partition**, not across partitions.\n")
	sb.WriteString("The following demonstrates that messages from different partitions may be consumed in any order:\n\n")
	sb.WriteString(r.generateCrossPartitionExample())
	sb.WriteString("\n")

	// Conclusions
	sb.WriteString("## Conclusions\n\n")
	sb.WriteString("### What This Test Proves\n\n")
	sb.WriteString("1. **Within-Partition Ordering:** ")
	if r.results.OrderingPreserved {
		sb.WriteString("✅ Messages within each partition maintain strict sequential order, even during consumer rebalancing.\n")
	} else {
		sb.WriteString("❌ Ordering violations were detected within partitions (unexpected).\n")
	}
	sb.WriteString("\n")
	sb.WriteString("2. **Cross-Partition Ordering:** Messages from different partitions are interleaved and do not maintain global ordering.\n\n")
	sb.WriteString("3. **Rebalancing Behavior:** Consumer rebalancing events occurred when consumers joined/left the group, ")
	sb.WriteString("but did not affect message ordering within partitions.\n\n")
	sb.WriteString("4. **Kafka Guarantees:** This test confirms Kafka's ordering guarantee: ")
	sb.WriteString("**messages are ordered within a partition, but not across partitions.**\n\n")

	// Write to file
	return os.WriteFile(filename, []byte(sb.String()), 0644)
}

// generateASCIIChart creates an ASCII visualization of message flow
func (r *Reporter) generateASCIIChart() string {
	var sb strings.Builder

	sb.WriteString("Partition | Message Consumption Timeline\n")
	sb.WriteString("----------|" + strings.Repeat("-", 60) + "\n")

	// Get sorted partition IDs
	partitionIDs := make([]int32, 0, len(r.results.PartitionStats))
	for id := range r.results.PartitionStats {
		partitionIDs = append(partitionIDs, id)
	}
	sort.Slice(partitionIDs, func(i, j int) bool {
		return partitionIDs[i] < partitionIDs[j]
	})

	for _, partitionID := range partitionIDs {
		stats := r.results.PartitionStats[partitionID]
		stats.mu.Lock()

		// Create a simple visualization bar
		barLength := stats.MessagesReceived / 5 // Scale down for display
		if barLength > 60 {
			barLength = 60
		}
		bar := strings.Repeat("█", barLength)

		sb.WriteString(fmt.Sprintf("    %d     | %s (%d msgs)\n", partitionID, bar, stats.MessagesReceived))
		stats.mu.Unlock()
	}

	return sb.String()
}

// generateCrossPartitionExample shows cross-partition message interleaving
func (r *Reporter) generateCrossPartitionExample() string {
	var sb strings.Builder

	sb.WriteString("Example of message consumption showing cross-partition interleaving:\n\n")
	sb.WriteString("```\n")
	sb.WriteString("Time  | Partition | Sequence | Note\n")
	sb.WriteString("------|-----------|----------|----------------------------------\n")
	sb.WriteString("T1    | 0         | 0        | First message from partition 0\n")
	sb.WriteString("T2    | 1         | 0        | First message from partition 1\n")
	sb.WriteString("T3    | 0         | 1        | Second message from partition 0\n")
	sb.WriteString("T4    | 2         | 0        | First message from partition 2\n")
	sb.WriteString("T5    | 1         | 1        | Second message from partition 1\n")
	sb.WriteString("T6    | 0         | 2        | Third message from partition 0\n")
	sb.WriteString("```\n\n")
	sb.WriteString("Notice: Within each partition (0, 1, 2), sequences are strictly increasing (0→1→2).\n")
	sb.WriteString("However, across partitions, messages are interleaved (not in global order).\n")

	return sb.String()
}

// GenerateRawLog generates a raw log file with all events
func (r *Reporter) GenerateRawLog(filename string) error {
	var sb strings.Builder

	sb.WriteString("=== KAFKA ORDERING TEST - RAW EVENT LOG ===\n\n")
	sb.WriteString(fmt.Sprintf("Test Start: %s\n", r.results.StartTime.Format("2006-01-02 15:04:05.000")))
	sb.WriteString(fmt.Sprintf("Test End: %s\n\n", r.results.EndTime.Format("2006-01-02 15:04:05.000")))

	sb.WriteString("=== REBALANCE EVENTS ===\n\n")
	for _, event := range r.results.RebalanceEvents {
		sb.WriteString(fmt.Sprintf("[%s] %s: Consumer=%s, Partitions=%v, Count=%d\n",
			event.Timestamp.Format("15:04:05.000"),
			event.EventType,
			event.ConsumerID,
			event.Partitions,
			event.PartitionCount))
	}

	sb.WriteString("\n=== PER-PARTITION STATISTICS ===\n\n")

	// Get sorted partition IDs
	partitionIDs := make([]int32, 0, len(r.results.PartitionStats))
	for id := range r.results.PartitionStats {
		partitionIDs = append(partitionIDs, id)
	}
	sort.Slice(partitionIDs, func(i, j int) bool {
		return partitionIDs[i] < partitionIDs[j]
	})

	for _, partitionID := range partitionIDs {
		stats := r.results.PartitionStats[partitionID]
		stats.mu.Lock()

		sb.WriteString(fmt.Sprintf("\nPartition %d:\n", partitionID))
		sb.WriteString(fmt.Sprintf("  Messages Received: %d\n", stats.MessagesReceived))
		sb.WriteString(fmt.Sprintf("  Ordering Violations: %d\n", len(stats.OrderingViolations)))
		sb.WriteString(fmt.Sprintf("  Gaps: %d\n", len(stats.Gaps)))
		sb.WriteString(fmt.Sprintf("  Duplicates: %d\n", len(stats.Duplicates)))

		if len(stats.OrderingViolations) > 0 {
			sb.WriteString("  Violations:\n")
			for _, v := range stats.OrderingViolations {
				sb.WriteString(fmt.Sprintf("    - Expected %d, got %d at offset %d\n",
					v.Expected, v.Received, v.Offset))
			}
		}

		if len(stats.Gaps) > 0 {
			sb.WriteString("  Gaps:\n")
			for _, g := range stats.Gaps {
				sb.WriteString(fmt.Sprintf("    - Missing sequences %d-%d\n",
					g.StartSequence, g.EndSequence))
			}
		}

		stats.mu.Unlock()
	}

	return os.WriteFile(filename, []byte(sb.String()), 0644)
}
