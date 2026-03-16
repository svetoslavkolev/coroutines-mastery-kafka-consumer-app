# Kafka Consumer with Coroutines

A Kotlin library for consuming Kafka messages with coroutines, providing automatic backpressure
handling, partition-level concurrency and robust offset management.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Threading Model](#threading-model)
- [Offset Management](#offset-management)
- [Backpressure Handling](#backpressure-handling)
- [Rebalance Handling](#rebalance-handling)
- [Shutdown Process](#shutdown-process)
- [Advanced Topics](#advanced-topics)
- [Best Practices](#best-practices)
- [Internal Architecture Details](#internal-architecture-details)
- [FAQ](#faq)

## Overview

This library wraps Apache Kafka's consumer client with Kotlin coroutines, providing:

- **Automatic backpressure** through partition-level pause/resume
- **Concurrent processing** of records across partitions
- **Robust offset management** with automatic commits and rebalance handling
- **No `max.poll.interval.ms` violations** - polling continues even when processing is slow
- **Graceful shutdown** with proper offset commits

## Key Features

### 🎯 Partition-Level Backpressure

When processing is slower than Kafka can deliver, the library automatically:

- Pauses individual partitions (not all consumption)
- Buffers records in partition-specific queues
- Resumes partitions when queues drain
- Continues polling to avoid triggering rebalances

### 🔄 Smart Offset Management

- Automatic periodic offset commits (configurable interval)
- Commits last processed offset during partition revocation
- Handles partial batch processing during cancellation
- Prevents duplicate processing after rebalances

### ⚡ Concurrent Processing

- Configurable concurrency level across all partitions
- Maintains ordering within each partition
- Parallel processing across different partitions

### 🛡️ Robust Error Handling

- Handles consumer kicked out of group (fencing)
- Proper cleanup on partition loss vs revocation
- Graceful shutdown with JVM shutdown hook
- Thread-safe operations with single-threaded Kafka dispatcher

## Architecture

The library consists of five core components working together:

```
┌─────────────────────────────────────────────────────────────┐
│                       Consumer                              │
│  - KafkaConsumer wrapper                                    │
│  - Single-threaded dispatcher for thread safety             │
│  - Rebalance event management                               │
│  - Shutdown coordination                                    │
└────────────────┬────────────────────────────────────────────┘
                 │
    ┌────────────┴───────────────┐
    │                            │
┌───▼────────┐         ┌─────────▼─────────┐
│   Poller   │         │  RebalanceFlow    │
│            │         │                   │
│ - Polls    │         │ - PartitionsAssigned
│   Kafka    │         │ - PartitionsRevoked
│ - Emits    │         │ - PartitionsLost
│   records  │         └─────────┬─────────┘
└───┬────────┘                   │
    │                            │
┌───▼────────────────────────────▼──────────────┐
│            QueueManager                       │
│  - Distributes records to partition queues    │
│  - Creates/removes queues on rebalance        │
│  - Triggers partition pause when queue full   │
└────────────────┬──────────────────────────────┘
                 │
         ┌───────┴─────────┐
         │  Partition      │
         │  Queues         │
         │  (Channels)     │
         └───────┬─────────┘
                 │
┌────────────────▼────────────────────────────────┐
│            Executor                             │
│  - Processes records from each queue            │
│  - Maintains partition-level concurrency        │
│  - Tracks last processed offset per partition   │
│  - Resumes partitions when queue empty          │
└────────────────┬────────────────────────────────┘
                 │
                 │ Offset notifications
                 ▼
┌─────────────────────────────────────────────────┐
│          OffsetManager                          │
│  - Tracks latest processed offsets              │
│  - Periodic offset commits                      │
│  - Handles offset commits during rebalance      │
│  - Cleans up offsets for lost partitions        │
└─────────────────────────────────────────────────┘
```

## How It Works

### Initialization Flow

1. **Consumer** is created with configuration
    - Creates single-threaded `kafkaDispatcher` for thread-safe access to Apache's KafkaConsumer
    - Subscribes to topics with rebalance listener
    - Registers JVM shutdown hook

2. **Poller** starts polling loop (lazy start)
    - Polls Kafka every `pollInterval`
    - Emits records downstream via `SharedFlow`

3. **QueueManager** observes records and rebalance events
    - Creates/removes partition queues based on partition assignments
    - Distributes incoming records to appropriate queues
    - Triggers partition pause when queues are full

4. **Executor** processes records from queues
    - Launches one coroutine per partition
    - Processes records with configured concurrency
    - Notifies offset manager after each batch
    - Resumes partitions when queues drain

5. **OffsetManager** tracks and commits offsets
    - Receives offset notifications from Executor
    - Commits offsets periodically
    - Commits offsets during partition revocation
    - Cleans up offsets on partition loss

### Normal Operation Flow

```
1. Poller polls Kafka → Gets ConsumerRecords
                 ↓
2. QueueManager receives records → Distributes to partition queues
                 ↓
3. If queue full → Pause partition at Kafka level
                 ↓
4. Executor processes records → Updates offset tracking
                 ↓
5. OffsetManager commits offsets → Periodically or on rebalance
                 ↓
6. Queue empties → Resume partition
```

### Rebalance Flow

#### Partitions Assigned

```
Kafka detects new assignment
        ↓
Next poll() detects the new assignment
        ↓
Consumer.onPartitionsAssigned callback
        ↓
RebalanceEvent.PartitionsAssigned emitted (async)
        ↓
QueueManager creates queues for new partitions
        ↓
Executor launches processing coroutines
        ↓
Processing begins
```

#### Partitions Revoked

```
Kafka initiates rebalance
        ↓
Next poll() detects partition revocation
        ↓
Consumer.onPartitionsRevoked callback
        ↓
RebalanceEvent.PartitionsRevoked emitted (async)
        ↓
QueueManager cancels queues for revoked partitions
        ↓
Executor jobs cancelled
        ↓
OffsetManager.onPartitionsRevoked (sync):
  1. Wait for executor jobs to complete
  2. Get snapshot of offsets
  3. Commit offsets synchronously
  4. Clean up offset tracking
        ↓
Consumer.onPartitionsRevoked callback completes
```

#### Partitions Lost (Consumer Kicked Out)

```
Consumer exceeds max.poll.interval.ms (should never happen with this library) or fenced by the broker
        ↓
Next poll() detects partition loss
        ↓
Consumer.onPartitionsLost callback
        ↓
RebalanceEvent.PartitionsLost emitted (async)
        ↓
QueueManager cancels queues
        ↓
Executor jobs cancelled
        ↓
OffsetManager.onPartitionsLost (sync):
  1. Wait for executor jobs to complete
  2. Clean up offset tracking (NO commit - it is not allowed)
        ↓
Consumer.onPartitionsLost callback completes
```

### Shutdown Flow

```
JVM shutdown OR fatal error
        ↓
Shutdown hook triggered
        ↓
1. Cancel backgroundScope → All coroutines stop
        ↓
2. Poller loop exits
        ↓
3. KafkaConsumer.close() triggers onPartitionsRevoked
        ↓
4. OffsetManager commits all pending offsets
        ↓
Shutdown completed
```

## Getting Started

### Basic Usage

```kotlin
import coroutines.mastery.kafka.consumer.library.builder.kafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

fun main() {
    val consumer = kafkaConsumer<String, String> {
        bootstrapServers("localhost:9092")
        consumerGroup("my-consumer-group")
        topics("my-topic")

        keyDeserializer(StringDeserializer::class)
        valueDeserializer(StringDeserializer::class)

        recordProcessing {
            processor { record ->
                // Your processing logic
                println("Processing: ${record.key()} -> ${record.value()}")
            }
        }
    }

    // Consumer runs in background until JVM shutdown
    Thread.sleep(Long.MAX_VALUE)
}
```

### Advanced Configuration

```kotlin
val consumer = kafkaConsumer<String, Customer> {
    bootstrapServers("localhost:9092")
    consumerGroup("customers-consumer-group")
    topics("customers", "orders")

    keyDeserializer(StringDeserializer::class)
    valueDeserializer(JsonDeserializer::class)

    polling {
        maxPollRecords(500)
        pollTimeout(100.milliseconds)
        pollInterval(500.milliseconds)
        maxPollInterval(5.minutes)
    }

    recordProcessing {
        queueSize(20)  // Buffer 20 batches per partition
        concurrency(10)  // Process up to 10 records concurrently

        processor { record ->
            processCustomer(record.value())
        }
    }

    offsetHandling {
        offsetCommitInterval(30.seconds)
        autoOffsetReset(OffsetHandlingConfig.AutoOffsetReset.EARLIEST)
    }
}
```

## Configuration

### ConsumerConfig

| Parameter           | Type                 | Default            | Description              |
|---------------------|----------------------|--------------------|--------------------------|
| `bootstrapServers`  | String               | **Required**       | Kafka broker addresses   |
| `consumerGroup`     | String               | **Required**       | Consumer group ID        |
| `topics`            | List<String>         | **Required**       | Topics to subscribe to   |
| `keyDeserializer`   | KClass<Deserializer> | StringDeserializer | Key deserializer class   |
| `valueDeserializer` | KClass<Deserializer> | StringDeserializer | Value deserializer class |

### PollingConfig

| Parameter         | Type     | Default   | Description                             |
|-------------------|----------|-----------|-----------------------------------------|
| `maxPollRecords`  | Int      | 500       | Max records per poll                    |
| `pollTimeout`     | Duration | 100ms     | Timeout for each poll call              |
| `pollInterval`    | Duration | 500ms     | Delay between poll calls                |
| `maxPollInterval` | Duration | 5 minutes | Max time between polls before rebalance |

### RecordProcessingConfig

| Parameter     | Type            | Default | Description                                            |
|---------------|-----------------|---------|--------------------------------------------------------|
| `queueSize`   | Int             | 10      | Buffer capacity per partition (in batches)             |
| `concurrency` | Int             | 5       | Max concurrent record processing across all partitions |
| `processor`   | RecordProcessor | No-op   | Your record processing logic                           |

### OffsetHandlingConfig

| Parameter              | Type            | Default | Description                                             |
|------------------------|-----------------|---------|---------------------------------------------------------|
| `offsetCommitInterval` | Duration        | 30s     | How often to commit offsets                             |
| `autoOffsetReset`      | AutoOffsetReset | LATEST  | What to do when no offset exists (EARLIEST/LATEST/NONE) |

## Error Handling

### User Processor Exceptions

**Your `RecordProcessor.process()` is responsible for all error handling.** The library does not
catch exceptions from your processor.

#### Current Library Behavior

If your processor throws an exception:

1. ✅ Processing stops for that partition
2. ✅ Last **successfully processed** offset is committed
3. ✅ Partition job completes
4. ⚠️ Failed record will be **redelivered** after rebalance (when partition is reassigned)
5. ⚠️ Other partitions continue processing normally

#### Recommended Patterns

##### 1. Handle All Errors (Recommended for Most Cases)

```kotlin
processor { record ->
    try {
        businessLogic(record)
    } catch (e: TransientException) {
        // Temporary failures - retry with backoff
        retryWithBackoff(maxAttempts = 3) {
            businessLogic(record)
        }
    } catch (e: PoisonPillException) {
        // Malformed data - log and skip
        log.error(e) {
            "Skipping poison pill at offset ${record.offset()}: ${e.message}"
        }
        deadLetterQueue.send(record)  // Optional: send to DLQ
        // Don't rethrow - processing continues
    } catch (e: Exception) {
        currentCoroutineContext().ensureActive() // required when working with coroutines
        log.error(e) { "Unexpected error at offset ${record.offset()}" }
        // Rethrow to stop partition or handle differently
        throw e
    }
}
```

##### 2. Stop on Any Error (Strict Mode)

```kotlin
processor { record ->
    // Any exception stops partition processing
    // Failed record will be redelivered after rebalance
    businessLogic(record)
}
```

##### 3. Retry with Backoff Helper

```kotlin
suspend fun <T> retryWithBackoff(
    maxAttempts: Int = 3,
    initialDelay: Duration = 100.milliseconds,
    maxDelay: Duration = 10.seconds,
    factor: Double = 2.0,
    block: suspend () -> T
): T {
    var currentDelay = initialDelay
    var lastException: Exception? = null

    repeat(maxAttempts) { attempt ->
        try {
            return block()
        } catch (e: Exception) {
            lastException = e
            if (attempt < maxAttempts - 1) {
                delay(currentDelay)
                currentDelay = (currentDelay * factor).coerceAtMost(maxDelay)
            }
        }
    }
    throw lastException!!
}
```

##### 4. Dead Letter Queue Pattern

```kotlin
val dlqProducer = KafkaProducer<String, String>(dlqConfig)

processor { record ->
    try {
        businessLogic(record)
    } catch (e: Exception) {
        log.error(e) { "Failed to process record at offset ${record.offset()}" }

        // Send to DLQ
        dlqProducer.send(
            ProducerRecord(
                "my-topic-dlq",
                record.key(),
                record.value()
            )
        )

        // Don't rethrow - processing continues
    }
}
```

### Kafka Consumer Exceptions

The library automatically handles exceptions from Kafka operations:

| Exception                      | Handling                    | Result                                                                                                                                                           |
|--------------------------------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `InterruptException`           | Logged, triggers shutdown   | Consumer closes gracefully                                                                                                                                       |
| `WakeupException`              | (Not used by library)       | N/A                                                                                                                                                              |
| `AuthenticationException`      | Logged, triggers shutdown   | Fatal error, consumer stops                                                                                                                                      |
| `AuthorizationException`       | Logged, triggers shutdown   | Fatal error, consumer stops                                                                                                                                      |
| `FencedInstanceIdException`    | Triggers `onPartitionsLost` | Offsets cleaned up, no commit                                                                                                                                    |
| `CommitFailedException`        | Logged as warning           | Expected when kicked out, offets cleaned up via `onPartitionsLost`                                                                                               |
| `RebalanceInProgressException` | Logged as warning           | Expected during rebalance. If rebalance event is `onPartitionsRevoked` - offsets are committed. If rebalance event is`onPartitionsLost` - offsets are cleaned up |

## Threading Model

### Single-Threaded Kafka Dispatcher

All Kafka consumer operations run on a **single-threaded dispatcher** to ensure thread safety:

```kotlin
private val kafkaDispatcher = Dispatchers.IO.limitedParallelism(1)
```

**Operations on kafkaDispatcher:**

- `poll()`
- `pause()`
- `resume()`
- `commitSync()`
- `close()`

### Multi-Threaded Record Processing

Record processing happens on a **limited parallelism dispatcher**:

```kotlin
private val dispatcher = Dispatchers.IO.limitedParallelism(concurrency)
```

**Processing characteristics:**

- Up to `concurrency` records processed simultaneously
- Across **all** partitions (not per-partition)
- Maintains ordering **within** each partition
- Each partition has its own dedicated coroutine

### Thread Safety Guarantees

1. **KafkaConsumer**: Never accessed concurrently (single-threaded dispatcher)
2. **Offset Map**: Protected by `Mutex` for concurrent access
3. **Partition Queues**: Thread-safe `Channel` operations
4. **Rebalance Callbacks**: Run on kafkaDispatcher thread (from `poll()`)

## Offset Management

### Commit Strategy

The library uses **manual offset commits** with multiple commit triggers:

#### 1. Periodic Commits

```kotlin
offsetHandling {
    offsetCommitInterval(30.seconds)  // Commit every 30 seconds
}
```

Commits all pending offsets for all partitions every 30 seconds (default).

#### 2. Partition Revocation Commits

When partitions are revoked during rebalance:

1. Executor jobs for revoked partitions are **cancelled**
2. Jobs emit their **last successfully processed offset** (in `finally` block with `NonCancellable`)
3. OffsetManager **waits** for jobs to complete
4. OffsetManager **commits offsets synchronously** for revoked partitions
5. Offsets are removed from tracking

**Critical**: Uses `commitSync()` (blocking) because the callback runs on kafkaDispatcher thread.

#### 3. Partition Loss Cleanup

When partitions are lost (consumer kicked out):

1. Executor jobs are cancelled and awaited
2. Offsets are **cleaned up** (not committed - not allowed when kicked out)
3. Processing state is reset

### Offset Commit Guarantees

#### Partial Batch Processing

If a batch is partially processed before cancellation:

```
Batch: [offset 100, 101, 102, 103, 104]
Processing 100 ✅ → Processing 101 ✅ → Processing 102 ✅ → Cancelled ❌
                                         ↑
                              Commits offset 103 (next to be consumed)
                              Records 103, 104 will be redelivered
```

The `finally` block with `NonCancellable` ensures the last processed offset is always committed.

#### Commit Ordering

Offsets are committed in this priority order:

1. **Partition revocation** (synchronous, before rebalance completes)
2. **Periodic commits** (background, may race with rebalance)
3. **Shutdown** (via partition revocation callback when closing consumer)

### Offset Tracking Synchronization

The library uses a `Mutex` to protect the offset map from concurrent access:

**Scenarios requiring synchronization:**

- Executor notifying new offsets
- Periodic commit reading offsets
- Rebalance callback reading/cleaning offsets

**Deadlock Prevention:**
The library releases the mutex **before** calling `commitSync()` to avoid deadlock:

```kotlin
// Periodic commit
val offsetsToCommit = mutex.withLock { nextOffsets.toMap() }
// Mutex released here
consumer.commitOffsets(offsetsToCommit)  // Can wait for kafkaDispatcher
mutex.withLock { /* cleanup */ }  // Reacquire for cleanup

// Rebalance callback (same pattern)
val offsetsToCommit = mutex.withLock { nextOffsets.filterKeys { it in partitions } }
// Mutex released here
consumer.commitOffsetsBlocking(offsetsToCommit)
mutex.withLock { /* cleanup */ }
```

## Backpressure Handling

### Partition-Level Pause/Resume

When processing can't keep up with Kafka:

1. **Queue Full**: QueueManager detects `Channel.trySend()` failure
   ```kotlin
   queues[partition]?.trySend(records)
       ?.onFailure {
           partitionsToPause.add(partition)
           resendToQueue(partition, records)  // Buffer and retry
       }
   ```

2. **Pause Partition**: Kafka stops fetching from this partition
   ```kotlin
   consumer.pause(partitionsToPause)
   ```

3. **Buffered Records**: Rejected records are stored and resent to queue
   ```kotlin
   backgroundScope.launch {
       queues[partition]?.send(rejectedRecords)  // Suspends until queue has space
   }
   ```

4. **Resume Partition**: When queue drains
   ```kotlin
   if (queue.isEmpty) {
       consumer.resume(listOf(partition))
   }
   ```

### Why This Prevents Rebalances

**Without partition-level backpressure:**

```
Processing slow → All partitions blocked → poll() not called → 
max.poll.interval.ms exceeded → Consumer kicked out → Rebalance
```

**With partition-level backpressure:**

```
Processing slow → Only slow partitions paused → poll() continues for other partitions → 
max.poll.interval.ms NOT exceeded → No rebalance
```

**Key insight**: `poll()` continues even when all partitions are paused. Kafka allows this, and it
keeps the consumer alive in the group.

### Configuring Backpressure

```kotlin
recordProcessing {
    queueSize(20)      // Each partition buffers up to 20 batches
    concurrency(10)    // Process up to 10 records concurrently
}

polling {
    maxPollRecords(500)      // Fetch up to 500 records per poll
    pollInterval(500.milliseconds)  // Poll every 500ms
}
```

**Tuning guidelines:**

- **Large `queueSize`**: More buffering, tolerates longer processing spikes
- **Small `queueSize`**: Less memory, faster pause/resume reactions
- **High `concurrency`**: Faster processing, more CPU/memory
- **Low `concurrency`**: Fewer resources, more predictable latency

## Rebalance Handling

### Types of Rebalances

#### 1. Cooperative Rebalance (PartitionsRevoked)

**Triggers:**

- New consumer joins group
- Consumer leaves group gracefully
- Consumer explicitly unsubscribes

**Behavior:**

- ✅ Offsets are committed synchronously
- ✅ Partition state is cleaned up
- ✅ Consumer remains in group
- ✅ May receive same or different partitions after rebalance

#### 2. Consumer Fenced (PartitionsLost)

**Triggers:**

- `max.poll.interval.ms` exceeded (processing too slow)
- Static member ID fencing (another consumer with same ID)
- Consumer crashed and rejoined

**Behavior:**

- ❌ Offsets **cannot** be committed (consumer not in group)
- ✅ Partition state is cleaned up
- ⚠️ Records processed since last commit will be **redelivered**
- ⚠️ Consumer stays alive, will rejoin group on next poll

## Shutdown Process

### Graceful Shutdown

The library registers a JVM shutdown hook that automatically:

1. Cancels all background coroutines
2. Triggers `onPartitionsRevoked` callback
3. Commits all pending offsets
4. Closes Kafka consumer

**No manual cleanup needed** - just let JVM shutdown normally.

### Fatal Error Shutdown

If a fatal error occurs during polling (authentication, authorization, unexpected errors):

1. Error is logged
2. `shutdownTask` is submitted to executor
3. Same graceful shutdown process runs
4. Application can exit cleanly

### Manual Shutdown

Currently, the library doesn't expose a public `close()` method. Shutdown happens via:

- JVM shutdown hook (normal termination)
- Fatal error handler (error scenarios)

## Advanced Topics

### Observing Records Flow

While not typically needed, you can observe the records flow:

```kotlin
val consumer = kafkaConsumer<String, String> { /* ... */ }

CoroutineScope(Dispatchers.Default).launch {
    consumer.records.observeRecords().collect { records ->
        println("Polled ${records.count()} records from Kafka")
        // Records are already being processed by your configured processor
        // This is just for monitoring/metrics
    }
}
```

### Observing Rebalances

```kotlin
val consumer = kafkaConsumer<String, String> { /* ... */ }

// Observe rebalance events
CoroutineScope(Dispatchers.Default).launch {
    consumer.rebalances.observeRebalanceEvents().collect { event ->
        when (event) {
            is RebalanceEvent.PartitionsAssigned -> {
                println("Got new partitions: ${event.partitions}")
            }
            is RebalanceEvent.PartitionsRevoked -> {
                println("Lost partitions (graceful): ${event.partitions}")
            }
            is RebalanceEvent.PartitionsLost -> {
                println("Lost partitions (fenced/kicked): ${event.partitions}")
                // Alert! Check max.poll.interval.ms configuration
            }
        }
    }
}
```

## Best Practices

### ✅ DO

1. **Handle errors in your processor** - Don't let exceptions propagate unless you want to stop
   partition processing
2. **Use retry logic for transient failures** - Network issues, temporary unavailability
3. **Skip poison pills** - Malformed data shouldn't block entire partition
4. **Monitor partition loss events** - Indicates consumer was kicked out (tune
   `max.poll.interval.ms`)
5. **Keep processing fast** - Slow processing → queue backlog → memory pressure
6. **Use appropriate queue size** - Balance between memory usage and pause/resume frequency

### ❌ DON'T

1. **Don't block threads** - Use suspending functions, not blocking I/O
2. **Don't access KafkaConsumer directly** - Library manages it for thread safety
3. **Don't commit offsets manually** - Library handles offset management
4. **Don't ignore PartitionsLost events** - They indicate configuration or performance issues
5. **Don't use very small queue sizes** - Causes excessive pause/resume cycles

## Internal Architecture Details

### Component Responsibilities

#### Consumer

- Wraps `KafkaConsumer<K, V>`
- Provides thread-safe operations via single-threaded dispatcher
- Manages rebalance listener and callbacks
- Coordinates shutdown

#### Poller

- Continuously polls Kafka in a loop
- Emits records via `SharedFlow` (started lazily)
- Handles poll exceptions and triggers shutdown on fatal errors

#### QueueManager

- Maintains one `Channel` per partition
- Distributes polled records to appropriate partition queues
- Creates queues on partition assignment
- Cancels queues on partition revocation/loss
- Triggers partition pause when queue is full
- Retries rejected records in separate coroutine

#### Executor

- One coroutine per partition (from partition queue)
- Processes records with configured concurrency limit
- Tracks last processed offset per partition
- Notifies OffsetManager after each batch
- Resumes partitions when queue empties
- Uses `NonCancellable` to ensure offset notification even during cancellation

#### OffsetManager

- Tracks next offset to commit per partition
- Launches periodic commit job
- Registers rebalance callback for commit/cleanup
- Handles commit failures gracefully

## FAQ

### Q: What happens if my processor suspends for a long time?

A: The executor job suspends, but:

- ✅ Polling continues (no `max.poll.interval.ms` violation)
- ✅ Other partitions continue processing
- ⚠️ This partition's queue may fill up → partition paused
- ⚠️ Offsets not committed until processing completes

### Q: Can I manually commit offsets?

A: No, the library manages offset commits. This ensures:

- No concurrent commit attempts
- Proper synchronization with rebalance
- No duplicate commits

### Q: What's the delivery guarantee?

A: **At-least-once delivery**:

- Records may be redelivered after rebalance
- Records may be redelivered if processing fails
- Offsets committed only after successful processing
- Partial batches commit last successful offset

### Q: Can I pause/resume partitions manually?

A: No, the library manages pause/resume automatically based on queue capacity. Manual control would
interfere with the backpressure mechanism.

### Q: Why are offsets committed even when processing fails?

A: Only **successfully processed** offsets are committed. If record 100 fails, offset 100 is
committed (not 101), so record 100 will be redelivered.
