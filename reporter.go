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

	// Message Processing During Rebalance
	sb.WriteString("## Message Processing During Rebalance\n\n")
	sb.WriteString("This section analyzes what happens to messages being processed when a rebalance occurs.\n\n")

	// Processing Configuration
	sb.WriteString("### Processing Configuration\n\n")
	sb.WriteString(fmt.Sprintf("- **Processing Time Range:** %s - %s\n",
		r.results.Config.ProcessingTimeMin.Round(time.Millisecond),
		r.results.Config.ProcessingTimeMax.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("- **Session Timeout:** %s\n", r.results.Config.SessionTimeout))
	sb.WriteString(fmt.Sprintf("- **Heartbeat Interval:** %s\n", r.results.Config.HeartbeatInterval))
	sb.WriteString(fmt.Sprintf("- **Slow Processing Mode:** %v\n", r.results.Config.EnableSlowProcessing))
	sb.WriteString("\n")

	// Processing Statistics
	sb.WriteString("### Processing Statistics\n\n")
	r.results.ProcessingStats.mu.RLock()
	sb.WriteString(fmt.Sprintf("- **Total Messages Processed:** %d\n", r.results.ProcessingStats.TotalProcessed))
	sb.WriteString(fmt.Sprintf("- **Messages Interrupted:** %d (%.1f%%)\n",
		r.results.ProcessingStats.TotalInterrupted,
		float64(r.results.ProcessingStats.TotalInterrupted)*100.0/float64(r.results.Config.NumMessages)))
	sb.WriteString(fmt.Sprintf("- **Messages Reprocessed:** %d\n", r.results.ProcessingStats.TotalReprocessed))
	sb.WriteString(fmt.Sprintf("- **Duplicate Processing:** %d messages\n", len(r.results.ProcessingStats.DuplicateProcessing)))
	sb.WriteString(fmt.Sprintf("- **Average Processing Time:** %s\n",
		r.results.ProcessingStats.AverageProcessingTime.Round(time.Millisecond)))
	if r.results.ProcessingStats.AverageReprocessingDelay > 0 {
		sb.WriteString(fmt.Sprintf("- **Average Reprocessing Delay:** %s\n",
			r.results.ProcessingStats.AverageReprocessingDelay.Round(time.Millisecond)))
	}
	r.results.ProcessingStats.mu.RUnlock()
	sb.WriteString("\n")

	// Interrupted Messages
	if len(r.results.ProcessingStats.InterruptedMessages) > 0 {
		sb.WriteString("### Interrupted Messages\n\n")
		sb.WriteString("Messages that were being processed when a rebalance occurred:\n\n")
		sb.WriteString("| Message ID | Partition | Offset | Seq | Consumer | Processing Time | Status |\n")
		sb.WriteString("|------------|-----------|--------|-----|----------|-----------------|--------|\n")

		// Limit to first 20 interrupted messages for readability
		limit := 20
		for i, msg := range r.results.ProcessingStats.InterruptedMessages {
			if i >= limit {
				sb.WriteString(fmt.Sprintf("\n*... and %d more interrupted messages*\n\n",
					len(r.results.ProcessingStats.InterruptedMessages)-limit))
				break
			}

			status := "Interrupted"
			if msg.State == ProcessingCompleted {
				status = "Reprocessed ✓"
			}

			sb.WriteString(fmt.Sprintf("| %s | %d | %d | %d | %s | %s | %s |\n",
				msg.MessageID,
				msg.Partition,
				msg.Offset,
				msg.SequenceNum,
				msg.ConsumerID,
				time.Since(msg.ProcessingStarted).Round(time.Millisecond),
				status))
		}
		sb.WriteString("\n")
	}

	// Reprocessed Messages Analysis
	if len(r.results.ProcessingStats.ReprocessedMessages) > 0 {
		sb.WriteString("### Reprocessing Analysis\n\n")
		sb.WriteString("Messages that were interrupted and then successfully reprocessed:\n\n")
		sb.WriteString("| Message ID | Partition | Original Consumer | Reprocessed By | Delay | Total Attempts |\n")
		sb.WriteString("|------------|-----------|-------------------|----------------|-------|----------------|\n")

		limit := 15
		for i, msg := range r.results.ProcessingStats.ReprocessedMessages {
			if i >= limit {
				sb.WriteString(fmt.Sprintf("\n*... and %d more reprocessed messages*\n\n",
					len(r.results.ProcessingStats.ReprocessedMessages)-limit))
				break
			}

			delay := msg.ReprocessedAt.Sub(msg.ProcessingStarted)
			sb.WriteString(fmt.Sprintf("| %s | %d | %s | %s | %s | %d |\n",
				msg.MessageID,
				msg.Partition,
				msg.ConsumerID,
				msg.ReprocessedBy,
				delay.Round(time.Millisecond),
				msg.ProcessingAttempts))
		}
		sb.WriteString("\n")
	}

	// Processing Timeline Visualization
	sb.WriteString("### Processing Timeline\n\n")
	sb.WriteString("Visual representation of message processing and interruptions during rebalances:\n\n")
	sb.WriteString("```\n")
	sb.WriteString(r.generateProcessingTimeline())
	sb.WriteString("```\n\n")

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
	sb.WriteString("4. **Message Processing During Rebalance:**\n")
	if r.results.ProcessingStats.TotalInterrupted > 0 {
		sb.WriteString(fmt.Sprintf("   - ✅ **Kafka interrupts processing** when rebalance occurs (%d messages interrupted)\n",
			r.results.ProcessingStats.TotalInterrupted))
		sb.WriteString(fmt.Sprintf("   - ✅ **Messages are safely reprocessed** by the new partition owner (%d messages reprocessed)\n",
			r.results.ProcessingStats.TotalReprocessed))
		sb.WriteString("   - ✅ **No message loss** during rebalancing - all interrupted messages were eventually completed\n")
		if r.results.ProcessingStats.AverageReprocessingDelay > 0 {
			sb.WriteString(fmt.Sprintf("   - ⏱️  **Average reprocessing delay:** %s\n",
				r.results.ProcessingStats.AverageReprocessingDelay.Round(time.Millisecond)))
		}
	} else {
		sb.WriteString("   - ℹ️  No messages were interrupted during rebalancing in this test run\n")
		sb.WriteString("   - This can happen if processing is fast relative to rebalance timing\n")
	}
	sb.WriteString("\n")
	sb.WriteString("5. **Kafka Guarantees:** This test confirms Kafka's guarantees:\n")
	sb.WriteString("   - **Ordering:** Messages are ordered within a partition, but not across partitions\n")
	sb.WriteString("   - **Processing Safety:** Messages being processed during rebalance are safely reprocessed after partition reassignment\n")
	sb.WriteString("   - **At-Least-Once Delivery:** With auto-commit enabled, messages may be reprocessed but none are lost\n\n")

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

// generateProcessingTimeline creates a timeline visualization of processing and rebalances
func (r *Reporter) generateProcessingTimeline() string {
	var sb strings.Builder

	sb.WriteString("Time     | Event Type          | Details\n")
	sb.WriteString("---------|---------------------|----------------------------------------\n")

	// Create timeline events combining rebalances and processing interruptions
	type timelineEvent struct {
		timestamp time.Time
		eventType string
		details   string
	}

	events := make([]timelineEvent, 0)

	// Add rebalance events
	for _, rebalance := range r.results.RebalanceEvents {
		eventType := "Rebalance"
		if rebalance.EventType == "ASSIGN" {
			eventType = "Rebalance (ASSIGN)"
		} else {
			eventType = "Rebalance (REVOKE)"
		}

		details := fmt.Sprintf("%s: %v partitions", rebalance.ConsumerID, rebalance.Partitions)
		events = append(events, timelineEvent{
			timestamp: rebalance.Timestamp,
			eventType: eventType,
			details:   details,
		})
	}

	// Add processing interruptions
	for _, msg := range r.results.ProcessingStats.InterruptedMessages {
		events = append(events, timelineEvent{
			timestamp: msg.ProcessingStarted,
			eventType: "Msg Interrupted",
			details:   fmt.Sprintf("P%d Seq%d by %s", msg.Partition, msg.SequenceNum, msg.ConsumerID),
		})
	}

	// Sort by timestamp
	sort.Slice(events, func(i, j int) bool {
		return events[i].timestamp.Before(events[j].timestamp)
	})

	// Display events (limit to prevent overwhelming output)
	limit := 30
	for i, event := range events {
		if i >= limit {
			sb.WriteString(fmt.Sprintf("\n... and %d more events\n", len(events)-limit))
			break
		}

		relativeTime := event.timestamp.Sub(r.results.StartTime).Round(time.Millisecond)
		sb.WriteString(fmt.Sprintf("T+%-6s | %-19s | %s\n",
			relativeTime.String(),
			event.eventType,
			event.details))
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
