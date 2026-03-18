package coroutines.mastery.kafka.consumer.library

import coroutines.mastery.kafka.consumer.library.config.RecordProcessingConfig
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream
import kotlin.reflect.KClass
import kotlin.test.*

@OptIn(ExperimentalCoroutinesApi::class)
class QueueManagerTest {

    private lateinit var poller: Poller<String, String>
    private lateinit var consumer: Consumer<String, String>

    private lateinit var rebalanceFlow: MutableSharedFlow<RebalanceEvent>
    private lateinit var recordsFlow: MutableSharedFlow<ConsumerRecords<String, String>>

    companion object {
        @JvmStatic
        fun rebalanceEvents(): Stream<Arguments> = Stream.of(
            Arguments.of(RebalanceEvent.PartitionsRevoked::class),
            Arguments.of(RebalanceEvent.PartitionsLost::class)
        )
    }

    @BeforeTest
    fun setup() {
        rebalanceFlow = MutableSharedFlow()
        consumer = mockk<Consumer<String, String>>()
        every { consumer.observeRebalanceEvents() } returns rebalanceFlow
        coEvery { consumer.pause(any()) } just Runs

        recordsFlow = MutableSharedFlow()
        poller = mockk<Poller<String, String>>()
        every { poller.observeRecords() } returns recordsFlow
    }

    @Test
    fun `should create queues when partitions are assigned`() = runTest {
        // Given
        val config = RecordProcessingConfig<String, String>(queueSize = 10)

        val partition0 = TopicPartition("test-topic", 0)
        val partition1 = TopicPartition("test-topic", 1)

        val lifecycleEvents = mutableListOf<QueueLifecycleEvent<String, String>>()

        val queueManager = QueueManager(poller, consumer, config, backgroundScope)

        val lifecycleJob = launch {
            queueManager.observeQueueLifecycle()
                .take(2)
                .toList(lifecycleEvents)
        }

        runCurrent()

        // When - Partitions assigned
        rebalanceFlow.emit(RebalanceEvent.PartitionsAssigned(listOf(partition0, partition1)))

        lifecycleJob.join()

        // Then
        assertEquals(2, lifecycleEvents.size)
        assertTrue(lifecycleEvents.all { it is QueueLifecycleEvent.QueueCreated })

        val createdPartitions = lifecycleEvents.map { it.partition }.toSet()
        assertTrue(createdPartitions.contains(partition0))
        assertTrue(createdPartitions.contains(partition1))
    }

    @ParameterizedTest
    @MethodSource("rebalanceEvents")
    fun `should remove and cancel queues when partitions are revoked or lost`(
        rebalanceEvent: KClass<RebalanceEvent>
    ) = runTest {
        // Given
        val config = RecordProcessingConfig<String, String>(queueSize = 10)

        val partition0 = TopicPartition("test-topic", 0)
        val partition1 = TopicPartition("test-topic", 1)

        val lifecycleEvents = mutableListOf<QueueLifecycleEvent<String, String>>()

        val queueManager = QueueManager(poller, consumer, config, backgroundScope)

        val lifecycleJob = launch {
            queueManager.observeQueueLifecycle()
                .take(4)  // 2 created + 2 removed
                .toList(lifecycleEvents)
        }

        runCurrent()

        // When - Assign then revoke/lost
        rebalanceFlow.emit(RebalanceEvent.PartitionsAssigned(listOf(partition0, partition1)))
        when (rebalanceEvent) {
            RebalanceEvent.PartitionsRevoked::class ->
                rebalanceFlow.emit(RebalanceEvent.PartitionsRevoked(listOf(partition0, partition1)))

            RebalanceEvent.PartitionsLost::class ->
                rebalanceFlow.emit(RebalanceEvent.PartitionsLost(listOf(partition0, partition1)))
        }

        lifecycleJob.join()

        // Then
        assertEquals(4, lifecycleEvents.size)
        assertEquals(2, lifecycleEvents.count { it is QueueLifecycleEvent.QueueCreated })
        assertEquals(2, lifecycleEvents.count { it is QueueLifecycleEvent.QueueRemoved })
    }

    @Test
    fun `should distribute records to correct partition queues`() = runTest {
        // Given
        val config = RecordProcessingConfig<String, String>(queueSize = 10)

        val partition0 = TopicPartition("test-topic", 0)
        val partition1 = TopicPartition("test-topic", 1)

        val lifecycleEvents = mutableListOf<QueueLifecycleEvent<String, String>>()

        val queueManager = QueueManager(poller, consumer, config, backgroundScope)

        val lifecycleJob = launch {
            queueManager.observeQueueLifecycle().take(2).toList(lifecycleEvents)
        }

        runCurrent()

        // Assign partitions first
        rebalanceFlow.emit(RebalanceEvent.PartitionsAssigned(listOf(partition0, partition1)))

        lifecycleJob.join()

        // Get the created queues
        val queue0 =
            (lifecycleEvents.find { it.partition == partition0 } as? QueueLifecycleEvent.QueueCreated)?.queue
        val queue1 =
            (lifecycleEvents.find { it.partition == partition1 } as? QueueLifecycleEvent.QueueCreated)?.queue
        assertNotNull(queue0)
        assertNotNull(queue1)

        // When - Emit records for both partitions
        val records0 = listOf(createConsumerRecord("key1", "value1", 0, partition0))
        val records1 = listOf(createConsumerRecord("key2", "value2", 0, partition1))
        val consumerRecords = createConsumerRecordsMultiPartition(
            mapOf(
                partition0 to records0,
                partition1 to records1
            )
        )

        recordsFlow.emit(consumerRecords)
        runCurrent()

        // Then - Records should be in correct queues
        val receivedRecords0 = queue0.tryReceive().getOrNull()
        val receivedRecords1 = queue1.tryReceive().getOrNull()

        assertNotNull(receivedRecords0)
        assertEquals(1, receivedRecords0.size)
        assertEquals("key1", receivedRecords0[0].key())
        assertEquals("value1", receivedRecords0[0].value())
        assertEquals(0, receivedRecords0[0].offset())

        assertNotNull(receivedRecords1)
        assertEquals(1, receivedRecords1.size)
        assertEquals("key2", receivedRecords1[0].key())
        assertEquals("value2", receivedRecords1[0].value())
        assertEquals(0, receivedRecords1[0].offset())

        // Verify no pause was triggered (queues had space)
        coVerify(exactly = 0) { consumer.pause(match { it.isNotEmpty() }) }
    }

    @Test
    fun `should pause partition when queue is full`() = runTest {
        // Given
        val config = RecordProcessingConfig<String, String>(queueSize = 2)  // Small queue

        val partition0 = TopicPartition("test-topic", 0)

        val lifecycleEvents = mutableListOf<QueueLifecycleEvent<String, String>>()

        val queueManager = QueueManager(poller, consumer, config, backgroundScope)

        val lifecycleJob = launch {
            queueManager.observeQueueLifecycle().take(1).toList(lifecycleEvents)
        }

        runCurrent()

        // Assign partition
        rebalanceFlow.emit(RebalanceEvent.PartitionsAssigned(listOf(partition0)))

        lifecycleJob.join()

        val queue = (lifecycleEvents[0] as QueueLifecycleEvent.QueueCreated).queue

        // When - Fill the queue (capacity = 2) and try to send one more
        val batch1 = listOf(createConsumerRecord("key1", "value1", 0, partition0))
        val batch2 = listOf(createConsumerRecord("key2", "value2", 1, partition0))
        val batch3 = listOf(createConsumerRecord("key3", "value3", 2, partition0))

        recordsFlow.emit(createConsumerRecords(partition0, batch1))
        runCurrent()

        recordsFlow.emit(createConsumerRecords(partition0, batch2))
        runCurrent()

        recordsFlow.emit(createConsumerRecords(partition0, batch3))  // Should trigger pause
        runCurrent()

        // Then - Partition should be paused
        coVerify(exactly = 1) { consumer.pause(match { partition0 in it }) }

        // Queue should have 2 batches
        assertNotNull(queue.tryReceive().getOrNull())
        assertNotNull(queue.tryReceive().getOrNull())

        // But also the third batch, after resending it when the channel got buffer space
        assertNotNull(queue.tryReceive().getOrNull())
        assertNull(queue.tryReceive().getOrNull())  // No more records should be in the queue
    }

    @Test
    fun `should pause only full partitions, not all partitions`() = runTest {
        // Given
        val config = RecordProcessingConfig<String, String>(queueSize = 1)  // Small queue

        val partition0 = TopicPartition("test-topic", 0)
        val partition1 = TopicPartition("test-topic", 1)

        QueueManager(poller, consumer, config, backgroundScope)
        runCurrent()

        // Assign partitions
        rebalanceFlow.emit(RebalanceEvent.PartitionsAssigned(listOf(partition0, partition1)))
        runCurrent()

        // When - Fill only partition0 queue, partition1 has space
        val batch01 = listOf(createConsumerRecord("key1", "value1", 0, partition0))
        val batch02 = listOf(createConsumerRecord("key2", "value2", 1, partition0))
        val batch1 = listOf(createConsumerRecord("key3", "value3", 0, partition1))

        recordsFlow.emit(createConsumerRecords(partition0, batch01))
        runCurrent()

        recordsFlow.emit(
            createConsumerRecordsMultiPartition(
                mapOf(
                    partition0 to batch02,  // This should be rejected (queue full)
                    partition1 to batch1      // This should be accepted
                )
            )
        )
        runCurrent()

        // Then - Pause should have been called
        coVerify(exactly = 1) { consumer.pause(match { partition0 in it }) }
        coVerify(exactly = 0) { consumer.pause(match { partition1 in it }) }
    }

    // Helper functions

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

    private fun createConsumerRecords(
        partition: TopicPartition,
        records: List<ConsumerRecord<String, String>>
    ): ConsumerRecords<String, String> {
        return ConsumerRecords(mapOf(partition to records), emptyMap())
    }

    private fun createConsumerRecordsMultiPartition(
        partitionRecords: Map<TopicPartition, List<ConsumerRecord<String, String>>>
    ): ConsumerRecords<String, String> {
        return ConsumerRecords(partitionRecords, emptyMap())
    }
}


