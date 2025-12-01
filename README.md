# Kafka Ordering Test Bench

A comprehensive Go-based test bench that demonstrates and proves Kafka's message ordering guarantees: **messages are ordered within partitions, but not across partitions**, even during consumer group rebalancing.

## Overview

This test bench uses the Sarama Kafka library to:

1. **Produce** sequentially numbered messages to 3 partitions
2. **Consume** messages using a consumer group with dynamic scaling (2→3→4→2 consumers)
3. **Trigger** multiple rebalancing events by adding/removing consumers
4. **Verify** that message ordering is preserved within each partition
5. **Demonstrate** that ordering is NOT guaranteed across partitions
6. **Generate** detailed reports with statistics and visualizations

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
                                  └────────────────────────────┘
```

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

4. **Per-Partition Statistics**
   - Messages received per partition
   - Any ordering violations (should be ZERO)
   - Gaps or duplicates detected
   - First and last message details

5. **Message Flow Visualization**
   - ASCII chart showing consumption pattern

6. **Cross-Partition Ordering Analysis**
   - Demonstrates lack of global ordering
   - Shows message interleaving across partitions

7. **Conclusions**
   - Summary of what the test proves
   - Kafka ordering guarantees explained

### Expected Results

**✅ Success Criteria:**
- ✅ Ordering preserved within partitions: **YES**
- ✅ Zero ordering violations per partition
- ✅ Messages consumed in sequence: 0, 1, 2, 3... within each partition
- ✅ Multiple rebalance events occurred without affecting ordering
- ✅ Cross-partition messages are interleaved (expected behavior)

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
MessageDelay: 10 * time.Millisecond,  // Delay between messages
```

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
├── consumer.go          # Consumer with rebalance handling
├── verifier.go          # Order verification logic
├── reporter.go          # Report generation (markdown & logs)
├── types.go             # Data structures and types
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
