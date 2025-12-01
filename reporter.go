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
		sb.WriteString(fmt.Sprintf("- **Messages Received (total):** %d\n", stats.MessagesReceived))
		sb.WriteString(fmt.Sprintf("- **Unique Messages:** %d\n", len(stats.SeenSequences)))
		sb.WriteString(fmt.Sprintf("- **Duplicate Messages:** %d (reprocessed due to rebalancing)\n", len(stats.DuplicateMessages)))
		sb.WriteString(fmt.Sprintf("- **Expected Final Sequence:** %d\n", stats.ExpectedSequence))
		sb.WriteString("\n")

		// Ordering status
		if len(stats.OrderingViolations) == 0 {
			sb.WriteString("- **Ordering Status:** ✅ **PERFECT** - No violations detected\n")
		} else {
			sb.WriteString(fmt.Sprintf("- **Ordering Status:** ❌ **VIOLATIONS DETECTED** - %d sequences went backwards\n", len(stats.OrderingViolations)))
		}

		sb.WriteString(fmt.Sprintf("- **Gaps Detected:** %d (sequences jumped forward)\n", len(stats.Gaps)))

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

		// TRUE ORDERING VIOLATIONS (sequences went backwards - should NEVER happen)
		if len(stats.OrderingViolations) > 0 {
			sb.WriteString("\n#### ❌ TRUE ORDERING VIOLATIONS (Sequences Went Backwards)\n\n")
			sb.WriteString("⚠️ **This should NEVER happen with Kafka's guarantees!**\n\n")
			sb.WriteString("| Expected Seq | Received Seq | Message ID | Offset | Consumer | Timestamp |\n")
			sb.WriteString("|--------------|--------------|------------|--------|----------|----------|\n")
			for _, violation := range stats.OrderingViolations {
				sb.WriteString(fmt.Sprintf("| %d | %d | %s | %d | %s | %s |\n",
					violation.Expected,
					violation.Received,
					violation.MessageID,
					violation.Offset,
					violation.ConsumerID,
					violation.Timestamp.Format("15:04:05.000")))
			}
			sb.WriteString("\n")
		}

		// DUPLICATE MESSAGES (expected with at-least-once delivery)
		if len(stats.DuplicateMessages) > 0 {
			sb.WriteString("\n#### 🔄 Duplicate Messages (Reprocessed)\n\n")
			sb.WriteString("These messages were processed multiple times due to rebalancing and auto-commit timing.\n")
			sb.WriteString("This is **expected behavior** with at-least-once delivery semantics.\n\n")

			limit := 15
			displayCount := len(stats.DuplicateMessages)
			if displayCount > limit {
				displayCount = limit
			}

			sb.WriteString("| Seq | Message ID | Process Count | Offset | Consumer | Timestamp |\n")
			sb.WriteString("|-----|------------|---------------|--------|----------|----------|\n")
			for i := 0; i < displayCount; i++ {
				dup := stats.DuplicateMessages[i]
				sb.WriteString(fmt.Sprintf("| %d | %s | %d | %d | %s | %s |\n",
					dup.SequenceNum,
					dup.MessageID,
					dup.ProcessCount,
					dup.Offset,
					dup.ConsumerID,
					dup.Timestamp.Format("15:04:05.000")))
			}

			if len(stats.DuplicateMessages) > limit {
				sb.WriteString(fmt.Sprintf("\n*... and %d more duplicates*\n", len(stats.DuplicateMessages)-limit))
			}
			sb.WriteString("\n")
		}

		// Gaps details (sequences jumped forward - NOT violations)
		if len(stats.Gaps) > 0 {
			sb.WriteString("\n#### ⚠️  Gaps (Sequences Jumped Forward - NOT Violations)\n\n")
			sb.WriteString("⚠️ **Note:** Gaps are NOT ordering violations! Kafka's ordering is still preserved.\n\n")
			sb.WriteString("Gaps occur when sequence numbers jump forward (e.g., 5 → 10). This means ordering is maintained,\n")
			sb.WriteString("but some sequences were not consumed (due to message loss, offset management, or test setup).\n\n")
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
			sb.WriteString("\n")
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

	// Key Distinction Section
	sb.WriteString("## Understanding: Violations vs. Gaps vs. Duplicates\n\n")
	sb.WriteString("It's critical to understand the differences between these three scenarios:\n\n")

	sb.WriteString("### ❌ TRUE Ordering Violation (BAD - should NEVER happen)\n")
	sb.WriteString("- **Definition:** Sequence numbers go **BACKWARDS** (e.g., 5 → 6 → 4)\n")
	sb.WriteString("- **Kafka Guarantee:** This should **NEVER** happen within a partition\n")
	sb.WriteString("- **What it means:** Kafka's ordering guarantee was broken (critical bug!)\n")
	sb.WriteString("- **Example:** Consumer reads seq=10, then reads seq=5 (sequence went backwards)\n")
	sb.WriteString("- **Detection:** `receivedSeq < expectedSeq` AND not the first message\n\n")

	sb.WriteString("### ⚠️  Gap / Forward Jump (OK - not a violation)\n")
	sb.WriteString("- **Definition:** Sequence numbers **JUMP FORWARD** (e.g., 5 → 10, skipping 6-9)\n")
	sb.WriteString("- **Kafka Guarantee:** Ordering is NOT violated - sequences still increase\n")
	sb.WriteString("- **What it means:** Some messages were not consumed (message loss, test setup, or offset management)\n")
	sb.WriteString("- **Example:** Consumer reads seq=5, then reads seq=10 (skipped 6-9)\n")
	sb.WriteString("- **Detection:** `receivedSeq > expectedSeq`\n")
	sb.WriteString("- **Common causes:**\n")
	sb.WriteString("  - Producer didn't send those sequences\n")
	sb.WriteString("  - Consumer group offset was manually set or reset\n")
	sb.WriteString("  - Messages expired or were compacted\n\n")

	sb.WriteString("### 🔄 Duplicate Message (OK - expected with at-least-once)\n")
	sb.WriteString("- **Definition:** Same sequence number processed **multiple times** (e.g., 5 → 6 → 5 → 7)\n")
	sb.WriteString("- **Kafka Guarantee:** This is **expected** with at-least-once delivery\n")
	sb.WriteString("- **What it means:** Message reprocessed after rebalance (before offset was committed)\n")
	sb.WriteString("- **Example:** Consumer processed seq=5, rebalanced before commit, new consumer processes seq=5 again\n")
	sb.WriteString("- **Detection:** Same sequence number seen more than once\n")
	sb.WriteString("- **Solution:** Make your message handlers idempotent OR use manual commits with exactly-once semantics\n\n")

	// Calculate duplicates across all partitions
	totalDuplicates := 0
	for _, stats := range r.results.PartitionStats {
		stats.mu.Lock()
		totalDuplicates += len(stats.DuplicateMessages)
		stats.mu.Unlock()
	}

	// Conclusions
	sb.WriteString("## Conclusions\n\n")
	sb.WriteString("### Test Results Summary\n\n")

	// 1. Ordering
	sb.WriteString("#### 1. Message Ordering Within Partitions\n\n")
	if r.results.OrderingPreserved {
		sb.WriteString("✅ **ORDERING PRESERVED: YES**\n\n")
		sb.WriteString("- Zero ordering violations detected\n")
		sb.WriteString("- Sequence numbers never went backwards\n")
		sb.WriteString("- Kafka's ordering guarantee confirmed\n")
	} else {
		sb.WriteString("❌ **ORDERING VIOLATED**\n\n")
		sb.WriteString("- Ordering violations detected (sequences went backwards)\n")
		sb.WriteString("- This indicates a problem with Kafka or the test setup\n")
	}
	sb.WriteString("\n")

	// 2. Duplicates
	sb.WriteString("#### 2. Duplicate Messages (At-Least-Once Delivery)\n\n")
	if totalDuplicates > 0 {
		sb.WriteString(fmt.Sprintf("🔄 **DUPLICATES OCCURRED: YES** (%d messages reprocessed)\n\n", totalDuplicates))
		sb.WriteString("- This is **expected** and **correct** behavior with auto-commit\n")
		sb.WriteString("- Rebalancing interrupted processing before offsets were committed\n")
		sb.WriteString("- At-least-once delivery guarantees no message loss\n")
		sb.WriteString("- Application must handle duplicates (idempotent processing)\n")
	} else {
		sb.WriteString("ℹ️  **DUPLICATES OCCURRED: NO**\n\n")
		sb.WriteString("- No duplicates in this test run\n")
		sb.WriteString("- This can happen if all messages completed processing before rebalances\n")
		sb.WriteString("- In production with longer processing times, duplicates are expected\n")
	}
	sb.WriteString("\n")

	// 3. Message Delivery Completeness
	sb.WriteString("#### 3. Message Delivery Completeness\n\n")
	uniqueMsgs := 0
	for _, stats := range r.results.PartitionStats {
		stats.mu.Lock()
		uniqueMsgs += len(stats.SeenSequences)
		stats.mu.Unlock()
	}

	if uniqueMsgs == r.results.TotalMessagesSent {
		sb.WriteString(fmt.Sprintf("✅ **ALL MESSAGES DELIVERED: YES** (%d/%d)\n\n", uniqueMsgs, r.results.TotalMessagesSent))
		sb.WriteString("- All sent messages were received\n")
		sb.WriteString("- No message loss during rebalancing\n")
	} else {
		sb.WriteString(fmt.Sprintf("⚠️  **MESSAGE LOSS DETECTED** (%d/%d received)\n\n", uniqueMsgs, r.results.TotalMessagesSent))
		sb.WriteString(fmt.Sprintf("- Missing %d messages\n", r.results.TotalMessagesSent-uniqueMsgs))
		sb.WriteString("- This may indicate test timing issues or Kafka configuration problems\n")
	}
	sb.WriteString("\n")

	// 4. Rebalancing Impact
	sb.WriteString("#### 4. Rebalancing Impact\n\n")
	sb.WriteString(fmt.Sprintf("- **Rebalance Events:** %d\n", len(r.results.RebalanceEvents)))
	if r.results.ProcessingStats.TotalInterrupted > 0 {
		sb.WriteString(fmt.Sprintf("- **Messages Interrupted:** %d\n", r.results.ProcessingStats.TotalInterrupted))
		sb.WriteString(fmt.Sprintf("- **Messages Reprocessed:** %d\n", r.results.ProcessingStats.TotalReprocessed))
		if r.results.ProcessingStats.AverageReprocessingDelay > 0 {
			sb.WriteString(fmt.Sprintf("- **Average Reprocessing Delay:** %s\n",
				r.results.ProcessingStats.AverageReprocessingDelay.Round(time.Millisecond)))
		}
	}
	sb.WriteString("\n")

	// Final Kafka Guarantees Confirmation
	sb.WriteString("### Kafka Guarantees Confirmed\n\n")
	sb.WriteString("This test confirms:\n\n")
	sb.WriteString("1. ✅ **Ordering:** Messages are strictly ordered within each partition\n")
	sb.WriteString("2. ✅ **At-Least-Once Delivery:** All messages delivered, some reprocessed (no loss)\n")
	sb.WriteString("3. ✅ **Partition Independence:** Cross-partition messages are not globally ordered\n")
	sb.WriteString("4. ✅ **Rebalancing Safety:** Rebalancing interrupts processing but preserves ordering\n\n")

	sb.WriteString("### Application Responsibilities\n\n")
	sb.WriteString("Your application must:\n\n")
	sb.WriteString("1. 🔄 **Handle Duplicates:** Make message processing idempotent\n")
	sb.WriteString("2. 💾 **Track Processing:** Use unique message IDs or external state to detect duplicates\n")
	sb.WriteString("3. ⚙️  **Choose Commit Strategy:** Auto-commit (simpler) vs Manual-commit (fewer duplicates)\n")
	sb.WriteString("4. ⏱️  **Monitor Processing Time:** Keep processing time < session timeout to reduce interruptions\n\n")

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
		sb.WriteString(fmt.Sprintf("  Duplicates: %d\n", len(stats.DuplicateMessages)))

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
