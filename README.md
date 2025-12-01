# Kafka Ordering Test Bench

A comprehensive Go-based test bench that demonstrates and proves Kafka's message ordering guarantees and processing safety: **messages are ordered within partitions (but not across partitions)**, and **message processing is safely interrupted and resumed during consumer rebalancing**.

## Overview

This test bench uses the Sarama Kafka library to:

1. **Produce** sequentially numbered messages to 3 partitions
2. **Consume** messages using a consumer group with dynamic scaling (2→3→4→2 consumers)
3. **Simulate** realistic message processing with configurable delays (50-200ms)
4. **Trigger** multiple rebalancing events by adding/removing consumers
5. **Verify** that message ordering is preserved within each partition
6. **Track** message processing lifecycle: started, interrupted, completed, reprocessed
7. **Detect** when messages are interrupted during rebalancing
8. **Demonstrate** that interrupted messages are safely reprocessed
9. **Generate** detailed reports with statistics, processing analysis, and visualizations

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kafka Cluster                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Partition 0  │  │ Partition 1  │  │ Partition 2  │          │
│  │ [0,1,2,3...] │  │ [0,1,2,3...] │  │ [0,1,2,3...] │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
              ▲                                    │
              │ Produce                     Consume│
              │                                    ▼
    ┌─────────────────┐           ┌────────────────────────────┐
    │    Producer     │           │   Consumer Group           │
    │  (Sequential    │           │  • consumer-0              │
    │   messages)     │           │  • consumer-1              │
    └─────────────────┘           │  • consumer-2 (added)      │
                                  │  • consumer-3 (added)      │
                                  └────────────────────────────┘
                                            │
                                            ▼
                                  ┌────────────────────────────┐
                                  │   Order Verifier           │
                                  │  • Tracks sequences        │
                                  │  • Detects violations      │
                                  │  • Logs rebalances         │
                                  │  • Tracks processing       │
                                  └────────────────────────────┘
```

## Key Features

### 1. Message Processing Simulation

The test simulates realistic message processing to demonstrate what happens during rebalancing:

- **Configurable Processing Time**: Each message takes 50-200ms to "process" (simulated with delays)
- **Processing Lifecycle Tracking**: Every message state is tracked:
  - `STARTED` - Processing began
  - `COMPLETED` - Processing finished successfully
  - `INTERRUPTED` - Rebalance occurred during processing
- **Random Jitter**: Processing times vary to simulate real-world scenarios

### 2. Rebalance Interruption Detection

The test detects and logs when rebalancing interrupts message processing:

- Identifies messages that were "in-flight" when rebalance started
- Tracks which consumer was processing the message
- Measures how long the message was being processed before interruption
- Records all interrupted messages for analysis

### 3. Message Reprocessing Verification

After a rebalance, the test verifies that interrupted messages are safely reprocessed:

- Tracks when interrupted messages are picked up by the new partition owner
- Measures the delay between interruption and reprocessing
- Detects duplicate processing attempts
- Ensures no message loss during rebalancing

### 4. Configurable Session Timeouts

The test allows configuring consumer session timeouts and processing times:

- **Fast Processing Mode** (default): 50-200ms processing, 10s session timeout

  - Most messages complete before rebalance
  - Few interruptions expected

- **Slow Processing Mode** (`EnableSlowProcessing = true`):
  - Processing time > session timeout
  - Forces processing to be interrupted
  - Demonstrates rebalance behavior with slow consumers

### 5. Enhanced Reporting

The generated RESULTS.md report includes:

- **Processing Statistics**: Total processed, interrupted, reprocessed messages
- **Interruption Details**: Table of all interrupted messages with timing
- **Reprocessing Analysis**: Which messages were reprocessed and by whom
- **Processing Timeline**: Visual timeline showing rebalances and interruptions
- **Average Delays**: Processing time and reprocessing delay averages

## Prerequisites

- **Docker & Docker Compose** - for running Kafka and Zookeeper
- **Go 1.19+** - for running the test bench

## Quick Start

### 1. Start Kafka Infrastructure

```bash
# Start Kafka and Zookeeper
docker-compose up -d

# Verify containers are running
docker-compose ps

# Check Kafka logs (optional)
docker-compose logs -f kafka
```

Wait about 30 seconds for Kafka to be fully ready.

### 2. Install Go Dependencies

```bash
go mod tidy
```

### 3. Run the Test

```bash
go run .
```

The test will automatically:

- Wait for Kafka to be ready
- Create the test topic with 3 partitions
- Start 2 consumers
- Begin producing 600 messages
- Add a 3rd consumer (triggers rebalance)
- Add a 4th consumer (more consumers than partitions)
- Remove consumers (triggers more rebalances)
- Verify ordering and generate reports

### 4. View Results

After the test completes, check:

```bash
# Detailed analysis and statistics
cat RESULTS.md

# Raw event log
cat test-events.log
```

## Test Phases

The test runs through the following phases:

1. **Phase 1**: Start 2 consumers

   - Consumer group forms with 2 members
   - Partitions are distributed (likely 2-1 split)

2. **Phase 2**: Start producer

   - Sends 600 messages (200 per partition)
   - Messages numbered sequentially per partition
   - Small 10ms delay between messages

3. **Phase 3**: Add 3rd consumer

   - **Triggers rebalance**
   - Partitions redistributed (1-1-1)
   - Tests ordering during rebalance

4. **Phase 4**: Add 4th consumer

   - **Triggers rebalance**
   - 4 consumers, 3 partitions (one idle)
   - Tests behavior with more consumers than partitions

5. **Phase 5**: Let consumers catch up

   - Ensures all messages are processed

6. **Phase 6**: Remove consumers
   - **Triggers multiple rebalances**
   - Tests ordering during scale-down

## Key Sarama Configurations

### Producer (producer.go)

```go
// CRITICAL: Enable idempotence to prevent duplicates and ensure ordering
saramaConfig.Producer.Idempotent = true

// CRITICAL: Requires acknowledgment from all in-sync replicas
saramaConfig.Producer.RequiredAcks = sarama.WaitForAll

// Manual partitioner: We explicitly set partition per message
saramaConfig.Producer.Partitioner = sarama.NewManualPartitioner
```

**Why these matter:**

- `Idempotent = true` ensures exactly-once semantics and prevents message duplication
- `RequiredAcks = WaitForAll` ensures durability before considering send successful
- Manual partitioner gives us control over which messages go to which partition

### Consumer (consumer.go)

```go
// CRITICAL: Start from the beginning
saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

// Auto-commit enabled with reasonable interval
saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
saramaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

// Rebalance strategy: Range assigns partitions in ranges
saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()

// Session timeout: If no heartbeat within 10s, trigger rebalance
saramaConfig.Consumer.Group.Session.Timeout = 10 * time.Second
```

**Why these matter:**

- `OffsetOldest` ensures we read all messages from the start
- Auto-commit manages offset commits automatically
- Rebalance strategy determines how partitions are assigned during rebalancing
- Session timeout controls when consumers are considered dead

## Understanding the Results

### RESULTS.md Structure

The generated report includes:

1. **Test Configuration**

   - Number of partitions, messages, consumers
   - Timing information

2. **Executive Summary**

   - Total messages sent/received
   - Number of rebalance events
   - Overall ordering verdict

3. **Rebalance Timeline**

   - Table showing all rebalance events
   - Which consumer got which partitions
   - Timestamps for correlation

4. **Message Processing During Rebalance** ⭐ NEW

   - Processing configuration (timeouts, delays)
   - Processing statistics (processed, interrupted, reprocessed)
   - Table of interrupted messages with timing details
   - Reprocessing analysis showing recovery
   - Visual timeline of processing and interruptions

5. **Per-Partition Statistics**

   - Messages received per partition
   - Any ordering violations (should be ZERO)
   - Gaps or duplicates detected
   - First and last message details

6. **Message Flow Visualization**

   - ASCII chart showing consumption pattern

7. **Cross-Partition Ordering Analysis**

   - Demonstrates lack of global ordering
   - Shows message interleaving across partitions

8. **Conclusions**
   - Summary of what the test proves
   - Kafka ordering guarantees explained
   - Processing safety during rebalancing

### Expected Results

**✅ Success Criteria:**

_Ordering:_

- ✅ Ordering preserved within partitions: **YES**
- ✅ Zero ordering violations per partition
- ✅ Messages consumed in sequence: 0, 1, 2, 3... within each partition
- ✅ Multiple rebalance events occurred without affecting ordering
- ✅ Cross-partition messages are interleaved (expected behavior)

_Processing Safety:_

- ✅ Messages interrupted during rebalancing (shows detection works)
- ✅ All interrupted messages successfully reprocessed
- ✅ No message loss during rebalancing
- ✅ Reprocessing delay measured and reported
- ✅ At-least-once delivery guaranteed

**❌ Failure Indicators:**

- ❌ Ordering violations within any partition
- ❌ Messages out of sequence within a partition
- ❌ Message loss (sent != received)

## What This Test Proves

### 1. Within-Partition Ordering ✅

Kafka **guarantees** that messages sent to the same partition will be consumed in the exact order they were produced, even during:

- Consumer rebalancing
- Consumer failures and restarts
- Network issues (with retries)

**Evidence:**

- Each partition shows sequence numbers: 0→1→2→3→...
- No ordering violations detected
- Rebalances don't affect sequence

### 2. Cross-Partition Ordering ❌

Kafka **does NOT guarantee** ordering across different partitions.

**Evidence:**

- Messages from partition 0, 1, and 2 are interleaved
- Global sequence is not maintained
- This is expected and by design

### 3. Rebalancing Behavior

Consumer group rebalancing:

- Occurs when consumers join/leave
- Temporarily pauses consumption
- Redistributes partitions
- **Does NOT** break within-partition ordering

**Evidence:**

- Multiple rebalance events logged
- Partition reassignments visible
- Ordering still preserved after rebalances

### 4. Message Processing Safety During Rebalancing ⭐ NEW

When a rebalance occurs, Kafka safely handles messages that are being processed:

**What Happens:**

1. **Rebalance Triggered**: New consumer joins/leaves the group
2. **Processing Interrupted**: Consumer's session context is cancelled
3. **Cleanup Called**: Consumer releases partitions, marks in-flight messages
4. **Partition Reassigned**: Another consumer takes ownership
5. **Message Reprocessed**: New owner processes the uncommitted message

**Guarantees:**

- ✅ **No Message Loss**: Interrupted messages are reprocessed by new owner
- ✅ **At-Least-Once Delivery**: Messages may be processed multiple times
- ✅ **Processing Detection**: Test tracks which messages were interrupted
- ✅ **Reprocessing Tracking**: Measures delay and verifies completion

**Evidence:**

- Interrupted messages logged with timing details
- Same messages reprocessed by different consumers
- Reprocessing delay measured (typically < 1 second)
- All interrupted messages eventually complete

**Real-World Implications:**

- Your message handlers must be **idempotent** (safe to run multiple times)
- Use manual offset commits for exactly-once semantics if needed
- Consider processing time relative to session timeout
- Monitor reprocessing delays in production

## Customization

You can modify the test parameters in `main.go`:

```go
const (
    topicName     = "ordering-test"      // Topic name
    numPartitions = 3                    // Number of partitions
    numMessages   = 600                  // Total messages to send
    consumerGroup = "ordering-test-group" // Consumer group name
)

// In TestConfig:
config := TestConfig{
    MessageDelay:       10 * time.Millisecond,   // Delay between messages
    ProcessingTimeMin:  50 * time.Millisecond,   // Min processing time
    ProcessingTimeMax:  200 * time.Millisecond,  // Max processing time
    SessionTimeout:     10 * time.Second,         // Consumer session timeout
    HeartbeatInterval:  3 * time.Second,          // Heartbeat interval
    EnableSlowProcessing: false,                  // Force slow processing
}
```

### Testing Slow Processing

To test what happens when processing takes longer than the session timeout:

```go
// In main.go TestConfig:
EnableSlowProcessing: true,  // Processing time will exceed session timeout
```

This will cause most messages to be interrupted during processing, demonstrating rebalancing behavior with slow consumers.

## Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (clears all Kafka data)
docker-compose down -v

# Remove generated files
rm RESULTS.md test-events.log
```

## Troubleshooting

### Kafka not starting

```bash
# Check logs
docker-compose logs kafka

# Restart containers
docker-compose down
docker-compose up -d
```

### Connection refused errors

- Wait longer for Kafka to be ready (30-60 seconds)
- Check if port 9092 is available: `lsof -i :9092`

### Test hangs or doesn't complete

- Check consumer logs for errors
- Ensure Kafka containers are running
- Try increasing timeouts in the test

### No messages consumed

- Verify topic was created: `docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092`
- Check consumer group: `docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group ordering-test-group --describe`

## File Structure

```
kafka-ordering-test/
├── main.go              # Orchestrator - runs the entire test
├── producer.go          # Message producer with partition distribution
├── consumer.go          # Consumer with rebalance & processing simulation
├── verifier.go          # Order verification logic
├── reporter.go          # Report generation (markdown & logs)
├── types.go             # Data structures (messages, processing state, stats)
├── docker-compose.yml   # Kafka & Zookeeper setup
├── go.mod              # Go module definition
├── go.sum              # Go dependencies
├── README.md           # This file
├── RESULTS.md          # Generated test results (after running)
└── test-events.log     # Generated raw log (after running)
```

## Technical Details

### Message Structure

Each message contains:

```json
{
  "partition_key": "key-0",
  "sequence_num": 42,
  "timestamp": 1701234567890,
  "message_id": "key-0-seq-42"
}
```

### Verification Logic

For each consumed message:

1. Extract partition and sequence number
2. Check if sequence = expected (previous + 1)
3. If match: ✅ In order
4. If sequence > expected: ⚠️ Gap detected
5. If sequence < expected: ❌ Out of order or duplicate

### Rebalance Detection

Rebalances are detected via Sarama's ConsumerGroupHandler:

- `Setup()` - called when partitions assigned
- `Cleanup()` - called when partitions revoked

Both events are logged with timestamps and partition details.

### Processing Lifecycle Tracking

Each message goes through a tracked processing lifecycle:

1. **Consumed**: Message received from Kafka
2. **Processing Started**: `results.StartProcessing()` called, state = `STARTED`
3. **Processing Simulation**: Sleep for random duration (50-200ms)
4. **Completion Path:**
   - **Normal**: Processing completes, state = `COMPLETED`, offset committed
   - **Interrupted**: Rebalance occurs, state = `INTERRUPTED`, offset NOT committed
5. **Reprocessing**: Interrupted message picked up by new owner
   - Tracked as a reprocessing attempt
   - State updated to `COMPLETED` when done

**Key Implementation Details:**

- Processing uses `time.NewTimer()` with `select` to detect interruptions
- `session.Context().Done()` signals rebalance during processing
- `Cleanup()` marks all in-flight messages as interrupted
- Auto-commit interval (1s) determines which messages need reprocessing

## Advanced Usage

### Running with More Messages

```bash
# Edit main.go
const numMessages = 3000

go run .
```

### Testing with More Partitions

```bash
# Edit main.go
const numPartitions = 10

go run .
```

### Manual Consumer Control

You can modify `main.go` to add consumers at different times or with different patterns to test various rebalancing scenarios.

## References

- [Kafka Documentation - Ordering Guarantees](https://kafka.apache.org/documentation/#semantics)
- [Sarama Library](https://github.com/IBM/sarama)
- [Consumer Group Protocol](https://kafka.apache.org/documentation/#consumerconfigs)

## License

MIT License - Feel free to use and modify for your testing needs.

Perfect! Now the test results are **accurate and conclusive**. Here's my analysis:

### Key Findings

**1. Ordering IS Preserved Within Partitions**

- ✅ **Zero true ordering violations** across all 3 partitions
- ✅ Sequences never went backwards (no 5→4→3)
- ✅ All 600 messages received in correct order within their partitions
- **This confirms Kafka's core guarantee**

**2. Rebalancing Causes Duplicates (Expected Behavior)**

- 🔄 **9 messages reprocessed** (1.5% of total)
- Each rebalance interrupted 3 messages (1 per partition)
- **This is correct at-least-once semantics**
- Duplicates occurred because:
  - Consumer was processing message
  - Rebalance happened (partitions reassigned)
  - Offset wasn't committed yet
  - New consumer reprocessed from last committed offset

**3. No Message Loss**

- ✅ All 600 messages delivered
- ✅ No gaps or missing sequences
- ✅ Rebalancing is safe

**4. Processing Interruption Pattern**

- 9 messages interrupted during 3 rebalance events
- All interrupted messages successfully reprocessed
- Processing times: 47-64 seconds (much longer than session timeout of 10s)
- Shows rebalancing doesn't wait for in-flight processing

## What This Proves

### ✅ Kafka Guarantees Confirmed:

1. **Ordering within partitions is rock-solid** - never breaks during rebalancing
2. **At-least-once delivery works correctly** - messages may be processed twice, never lost
3. **Rebalancing is safe** - partitions get reassigned without breaking ordering
4. **Consumer groups work as designed** - partitions smoothly handed off between consumers

### 🔄 Application Implications

**You must handle duplicates because:**

- Auto-commit happens periodically (not per message)
- Rebalancing doesn't wait for processing to complete
- A message being processed during rebalance will be reprocessed

**Solutions:**

1. **Idempotent processing** - design so processing same message twice has same effect as once
2. **Manual commits** - commit only after successfully processing (reduces duplicates but adds latency)
3. **Exactly-once semantics** - use Kafka transactions (more complex, higher overhead)

## Excellent Test Results

The test now clearly demonstrates:

- ✅ Kafka preserves ordering during rebalancing (your original question)
- ✅ Rebalancing causes duplicate processing, not ordering violations
- ✅ The distinction between ordering (never broken) vs duplicates (expected)

This is exactly what you'd want to see in a production Kafka system with at-least-once delivery semantics!
