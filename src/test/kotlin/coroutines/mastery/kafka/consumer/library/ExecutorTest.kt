package coroutines.mastery.kafka.consumer.library

import coroutines.mastery.kafka.consumer.library.config.RecordProcessingConfig
import io.mockk.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.job
import kotlinx.coroutines.test.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.assertDoesNotThrow
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class ExecutorTest {

    private lateinit var consumer: Consumer<String, String>
    private lateinit var queueManager: QueueManager<String, String>
    private lateinit var processor: RecordProcessor<String, String>
    private lateinit var config: RecordProcessingConfig<String, String>
    private lateinit var queueLifecycleFlow: MutableSharedFlow<QueueLifecycleEvent<String, String>>
    private lateinit var testDispatcher: TestDispatcher

    @BeforeTest
    fun setup() {
        testDispatcher = StandardTestDispatcher()
        consumer = mockk<Consumer<String, String>>()
        queueManager = mockk<QueueManager<String, String>>()
        processor = mockk<RecordProcessor<String, String>>()
        config = RecordProcessingConfig(
            queueSize = 10,
            concurrency = 5,
            processor = processor
        )

        queueLifecycleFlow = MutableSharedFlow()
        every { queueManager.observeQueueLifecycle() } returns queueLifecycleFlow
        coEvery { consumer.resume(any()) } just Runs
        coEvery { processor.process(any()) } just Runs
    }

    @Test
    fun `should start processing job when queue is created`() = runTest(testDispatcher) {
        // Given
        val partition = TopicPartition("test-topic", 0)
        val queue = Channel<List<ConsumerRecord<String, String>>>(10)

        Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        runCurrent()

        // When - Queue created event
        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
        runCurrent()

        val job = backgroundScope.coroutineContext.job.children
            .find { it.toString().contains("executor-partition-0") }
        assertNotNull(job)
        assertTrue { job.isActive }

        // Send a record to verify job is processing
        val record = createConsumerRecord("key1", "value1", 0, partition)
        queue.send(listOf(record))
        runCurrent()

        // Then - Record should be processed
        coVerify(exactly = 1) { processor.process(record) }
    }

    @Test
    fun `should cancel processing job when queue is removed`() = runTest(testDispatcher) {
        // Given
        val partition = TopicPartition("test-topic", 0)
        val queue = Channel<List<ConsumerRecord<String, String>>>(10)

        Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        runCurrent()

        // Create queue and start job
        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
        runCurrent()

        val job = backgroundScope.coroutineContext.job.children
            .find { it.toString().contains("executor-partition-0") }
        assertNotNull(job)
        assertTrue { job.isActive }

        // When - Queue removed
        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueRemoved(partition))
        runCurrent()

        // Then - job is canceled and removed from children
        assertTrue { job.isCancelled }
        assertNull(
            backgroundScope.coroutineContext.job.children
                .find { it.toString().contains("executor-partition-0") }
        )
    }

    @Test
    fun `should process records from queue in order`() = runTest(testDispatcher) {
        // Given
        val partition = TopicPartition("test-topic", 0)
        val queue = Channel<List<ConsumerRecord<String, String>>>(10)
        val processedRecords = mutableListOf<ConsumerRecord<String, String>>()

        coEvery { processor.process(any()) } answers {
            processedRecords.add(firstArg())
        }

        Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        runCurrent()

        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
        runCurrent()

        // When - Send multiple batches
        val record1 = createConsumerRecord("key1", "value1", 0, partition)
        val record2 = createConsumerRecord("key2", "value2", 1, partition)
        val record3 = createConsumerRecord("key3", "value3", 2, partition)

        queue.send(listOf(record1, record2))
        runCurrent()

        queue.send(listOf(record3))
        runCurrent()

        // Then - All records processed in order
        assertEquals(3, processedRecords.size)
        assertEquals(0L, processedRecords[0].offset())
        assertEquals(1L, processedRecords[1].offset())
        assertEquals(2L, processedRecords[2].offset())
    }

    @Test
    fun `should notify offset notifiers after processing each batch`() = runTest(testDispatcher) {
        // Given
        val partition = TopicPartition("test-topic", 0)
        val queue = Channel<List<ConsumerRecord<String, String>>>(10)
        val notifiedOffsets = mutableListOf<LatestProcessedOffset>()
        val notifier = OffsetNotifier { offset ->
            notifiedOffsets.add(offset)
        }

        val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        executor.registerOffsetNotifier(notifier)
        runCurrent()

        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
        runCurrent()

        // When - Process batch with offset 0 and 1
        val record1 = createConsumerRecord("key1", "value1", 0, partition)
        val record2 = createConsumerRecord("key2", "value2", 1, partition)
        queue.send(listOf(record1, record2))
        runCurrent()

        // Then - Should notify with offset 1 (last processed)
        assertEquals(1, notifiedOffsets.size)
        assertEquals(1L, notifiedOffsets[0].latestOffset)
        assertEquals(partition, notifiedOffsets[0].partition)

        // When - Process another batch with offset 2
        val record3 = createConsumerRecord("key3", "value3", 2, partition)
        queue.send(listOf(record3))
        runCurrent()

        // Then - Should notify with offset 2
        assertEquals(2, notifiedOffsets.size)
        assertEquals(2L, notifiedOffsets[1].latestOffset)
        assertEquals(partition, notifiedOffsets[1].partition)
    }

    @Test
    fun `should resume partition when queue becomes empty after processing`() =
        runTest(testDispatcher) {
            // Given
            val partition = TopicPartition("test-topic", 0)
            val queue = Channel<List<ConsumerRecord<String, String>>>(10)

            Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
            runCurrent()

            queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
            runCurrent()

            // When - Send records
            val record = createConsumerRecord("key1", "value1", 0, partition)
            queue.send(listOf(record))
            runCurrent()

            // Then - Should resume partition after queue is empty
            coVerify(exactly = 1) { consumer.resume(listOf(partition)) }
        }

    @Test
    fun `should handle multiple partitions independently`() = runTest(testDispatcher) {
        // Given
        val partition0 = TopicPartition("test-topic", 0)
        val partition1 = TopicPartition("test-topic", 1)
        val queue0 = Channel<List<ConsumerRecord<String, String>>>(10)
        val queue1 = Channel<List<ConsumerRecord<String, String>>>(10)

        val processedRecords = mutableListOf<ConsumerRecord<String, String>>()
        coEvery { processor.process(any()) } answers {
            processedRecords.add(firstArg())
        }

        val notifiedOffsets = mutableListOf<LatestProcessedOffset>()
        val offsetNotifier = OffsetNotifier { offset ->
            notifiedOffsets.add(offset)
        }

        val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        executor.registerOffsetNotifier(offsetNotifier)
        runCurrent()

        // When - Create queues for both partitions
        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition0, queue0))
        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition1, queue1))
        runCurrent()

        // Send records to both partitions
        val record0 = createConsumerRecord("key0", "value0", 0, partition0)
        val record1 = createConsumerRecord("key1", "value1", 0, partition1)

        queue0.send(listOf(record0))
        queue1.send(listOf(record1))
        runCurrent()

        // Then - Both partitions processed independently
        assertEquals(2, processedRecords.size)
        assertTrue { processedRecords.any { it.partition() == partition0.partition() } }
        assertTrue { processedRecords.any { it.partition() == partition1.partition() } }

        assertEquals(2, notifiedOffsets.size)
        assertTrue { notifiedOffsets.any { it.partition == partition0 } }
        assertTrue { notifiedOffsets.any { it.partition == partition1 } }
    }

    @Test
    fun `should notify offset in NonCancellable context during cancellation`() =
        runTest(testDispatcher) {
            // Given
            val partition = TopicPartition("test-topic", 0)
            val queue = Channel<List<ConsumerRecord<String, String>>>(10)
            val notifiedOffsets = mutableListOf<LatestProcessedOffset>()
            val notifier = OffsetNotifier { offset ->
                notifiedOffsets.add(offset)
            }

            coEvery { processor.process(any()) } coAnswers {
                delay(100)
            }

            val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
            executor.registerOffsetNotifier(notifier)
            runCurrent()

            queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
            runCurrent()

            // When - Start processing then cancel via queue removal
            val record1 = createConsumerRecord("key1", "value1", 100, partition)
            val record2 = createConsumerRecord("key2", "value2", 101, partition)
            queue.send(listOf(record1, record2))

            advanceTimeBy(150)
            runCurrent()

            queueLifecycleFlow.emit(QueueLifecycleEvent.QueueRemoved(partition))
            runCurrent()

            // Then - Should notify offset 100 as executor job was canceled while processing offset 101
            assertEquals(1, notifiedOffsets.size)
            assertEquals(100L, notifiedOffsets[0].latestOffset)
        }

    @Test
    fun `should handle multiple offset notifiers`() = runTest(testDispatcher) {
        // Given
        val partition = TopicPartition("test-topic", 0)
        val queue = Channel<List<ConsumerRecord<String, String>>>(10)

        val notifiedOffsets1 = mutableListOf<LatestProcessedOffset>()
        val notifiedOffsets2 = mutableListOf<LatestProcessedOffset>()

        val notifier1 = OffsetNotifier { offset -> notifiedOffsets1.add(offset) }
        val notifier2 = OffsetNotifier { offset -> notifiedOffsets2.add(offset) }

        val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        executor.registerOffsetNotifier(notifier1)
        executor.registerOffsetNotifier(notifier2)
        runCurrent()

        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
        runCurrent()

        // When - Process records
        val record = createConsumerRecord("key1", "value1", 0, partition)
        queue.send(listOf(record))
        runCurrent()

        // Then - Both notifiers should receive the offset
        assertEquals(1, notifiedOffsets1.size)
        assertEquals(1, notifiedOffsets2.size)
        assertEquals(notifiedOffsets1[0], notifiedOffsets2[0])
    }

    @Test
    fun `should not fail when awaiting jobs for partitions with no jobs`() =
        runTest(testDispatcher) {
            // Given
            val partition = TopicPartition("test-topic", 0)
            val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
            runCurrent()

            // When - Await non-existent partition job
            // Then - Should complete without errors
            assertDoesNotThrow { executor.awaitPartitionJobs(listOf(partition)) }
        }

    @Test
    fun `should process records from multiple partitions concurrently with configured parallelism`() =
        runTest(testDispatcher) {
            // Given
            val partition0 = TopicPartition("test-topic", 0)
            val partition1 = TopicPartition("test-topic", 1)
            val partition2 = TopicPartition("test-topic", 2)

            val queue0 = Channel<List<ConsumerRecord<String, String>>>(10)
            val queue1 = Channel<List<ConsumerRecord<String, String>>>(10)
            val queue2 = Channel<List<ConsumerRecord<String, String>>>(10)

            coEvery { processor.process(any()) } coAnswers {
                delay(100)
            }

            val customConfig = RecordProcessingConfig(
                concurrency = 3,
                processor = processor
            )

            Executor(consumer, queueManager, customConfig, backgroundScope, testDispatcher)
            runCurrent()

            queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition0, queue0))
            queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition1, queue1))
            queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition2, queue2))
            runCurrent()

            // When - Send records to all partitions
            val record0 = createConsumerRecord("key0", "value0", 0, partition0)
            val record1 = createConsumerRecord("key1", "value1", 0, partition1)
            val record2 = createConsumerRecord("key2", "value2", 0, partition2)

            queue0.send(listOf(record0))
            queue1.send(listOf(record1))
            queue2.send(listOf(record2))

            advanceTimeBy(100)
            runCurrent()

            // Then - All records should be processed in parallel in 100ms
            coVerify(exactly = 3) { processor.process(any()) }
            assertEquals(100, currentTime)
        }

    @Test
    fun `should process empty batch without errors`() = runTest(testDispatcher) {
        // Given
        val partition = TopicPartition("test-topic", 0)
        val queue = Channel<List<ConsumerRecord<String, String>>>(10)
        val offsetNotifier = mockk<OffsetNotifier>()

        val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        executor.registerOffsetNotifier(offsetNotifier)
        runCurrent()

        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
        runCurrent()

        // When - Send empty batch
        queue.send(emptyList())
        runCurrent()

        // Then - Should not process anything
        coVerify(exactly = 0) { processor.process(any()) }
        coVerify(exactly = 0) { offsetNotifier.notifyLatestOffset(any()) }
    }

    @Test
    fun `should notify offsets in order within a batch`() = runTest(testDispatcher) {
        // Given
        val partition = TopicPartition("test-topic", 0)
        val queue = Channel<List<ConsumerRecord<String, String>>>(10)

        val notifiedOffsets = mutableListOf<LatestProcessedOffset>()
        val notifier = OffsetNotifier { offset -> notifiedOffsets.add(offset) }

        val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        executor.registerOffsetNotifier(notifier)
        runCurrent()

        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
        runCurrent()

        // When - Process batch with offsets in order
        val records = listOf(
            createConsumerRecord("key5", "value5", 5, partition),
            createConsumerRecord("key6", "value6", 6, partition),
            createConsumerRecord("key7", "value7", 7, partition)
        )
        queue.send(records)
        runCurrent()

        // Then - Should notify once with the last offset (7)
        assertEquals(1, notifiedOffsets.size)
        assertEquals(7L, notifiedOffsets[0].latestOffset)
    }

    @Test
    fun `should process multiple batches from same partition sequentially`() =
        runTest(testDispatcher) {
            // Given
            val partition = TopicPartition("test-topic", 0)
            val queue = Channel<List<ConsumerRecord<String, String>>>(10)

            val notifiedOffsets = mutableListOf<LatestProcessedOffset>()
            val notifier = OffsetNotifier { offset -> notifiedOffsets.add(offset) }

            coEvery { processor.process(any()) } coAnswers {
                delay(100)
            }

            val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
            executor.registerOffsetNotifier(notifier)
            runCurrent()

            queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
            runCurrent()

            // When - Send 3 batches
            queue.send(listOf(createConsumerRecord("key1", "value1", 0, partition)))
            queue.send(listOf(createConsumerRecord("key2", "value2", 1, partition)))
            queue.send(listOf(createConsumerRecord("key3", "value3", 2, partition)))

            // Then - batch1 is finished after 100ms
            runCurrent()
            advanceTimeBy(100)
            coVerify(exactly = 1) { processor.process(any()) }

            // Then - batch2 is finished after another 100ms
            runCurrent()
            advanceTimeBy(100)
            coVerify(exactly = 2) { processor.process(any()) }

            // Then - batch3 is finished after another 100ms
            advanceTimeBy(100)
            runCurrent()
            coVerify(exactly = 3) { processor.process(any()) }

            // Then - batches are processed sequentially, 3 batches for 100ms each
            assertEquals(300, currentTime)

            // Then - Should notify 3 times in order
            assertEquals(3, notifiedOffsets.size)
            assertEquals(0L, notifiedOffsets[0].latestOffset)
            assertEquals(1L, notifiedOffsets[1].latestOffset)
            assertEquals(2L, notifiedOffsets[2].latestOffset)
        }

    @Test
    fun `should handle processor throwing CancellationException`() = runTest(testDispatcher) {
        // Given
        val partition = TopicPartition("test-topic", 0)
        val queue = Channel<List<ConsumerRecord<String, String>>>(10)

        val notifiedOffsets = mutableListOf<LatestProcessedOffset>()
        val notifier = OffsetNotifier { offset -> notifiedOffsets.add(offset) }

        var processCount = 0
        coEvery { processor.process(any()) } coAnswers {
            if (processCount++ == 1) {
                throw CancellationException("Cancelled")
            }
        }

        val executor = Executor(consumer, queueManager, config, backgroundScope, testDispatcher)
        executor.registerOffsetNotifier(notifier)
        runCurrent()

        queueLifecycleFlow.emit(QueueLifecycleEvent.QueueCreated(partition, queue))
        runCurrent()

        val job = backgroundScope.coroutineContext.job.children
            .find { it.toString().contains("executor-partition-0") }
        assertNotNull(job)
        assertTrue { job.isActive }

        // When - Process batch where second record throws CancellationException
        val records = listOf(
            createConsumerRecord("key1", "value1", 0, partition),
            createConsumerRecord("key2", "value2", 1, partition),
            createConsumerRecord("key3", "value3", 2, partition)
        )
        queue.send(records)
        runCurrent()

        // Then - Should notify with offset 0 (last successfully processed before failure)
        assertEquals(1, notifiedOffsets.size)
        assertEquals(0L, notifiedOffsets[0].latestOffset)
        assertEquals(partition, notifiedOffsets[0].partition)

        // Then - job is canceled and removed from children
        assertTrue { job.isCancelled }
        assertNull(
            backgroundScope.coroutineContext.job.children
                .find { it.toString().contains("executor-partition-0") }
        )
    }

    private fun createConsumerRecord(
        key: String,
        value: String,
        offset: Long,
        partition: TopicPartition
    ): ConsumerRecord<String, String> {
        return ConsumerRecord(
            partition.topic(),
            partition.partition(),
            offset,
            key,
            value
        )
    }
}
