package coroutines.mastery.kafka.consumer.library

import coroutines.mastery.kafka.consumer.library.config.PollingConfig
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class PollerTest {

    @Test
    fun `should start polling only when first collector subscribes (Lazily)`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollingConfig = PollingConfig(
            pollTimeout = 100.milliseconds,
            pollInterval = 500.milliseconds
        )

        coEvery { consumer.poll(any()) } returns emptyConsumerRecords()

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When - Poller created but no collectors yet
        advanceTimeBy(1000)

        // Then - No polling should have happened yet
        coVerify(exactly = 0) { consumer.poll(any()) }

        // When - First collector subscribes
        val job = launch {
            poller.observeRecords().collect()
        }

        advanceTimeBy(100)

        // Then - Polling should have started
        coVerify(exactly = 1) { consumer.poll(any()) }

        job.cancelAndJoin()
    }

    @Test
    fun `should emit non-empty records and skip empty polls`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollingConfig = PollingConfig(
            pollTimeout = 100.milliseconds,
            pollInterval = 100.milliseconds
        )

        val record1 = createConsumerRecord("key1", "value1", 0)
        val record2 = createConsumerRecord("key2", "value2", 1)
        val records1 = createConsumerRecords(listOf(record1))
        val records2 = createConsumerRecords(listOf(record2))

        // Return: records -> empty -> records -> empty
        coEvery { consumer.poll(any()) } returnsMany listOf(
            records1,
            emptyConsumerRecords(),
            records2,
            emptyConsumerRecords()
        )

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When
        val collectedRecords = mutableListOf<ConsumerRecords<String, String>>()
        launch {
            poller.observeRecords().take(2).toList(collectedRecords)
        }

        advanceTimeBy(1000)

        // Then - Should only emit non-empty records
        assertEquals(2, collectedRecords.size)
        assertEquals(1, collectedRecords[0].count())
        assertEquals(1, collectedRecords[1].count())

        with(collectedRecords[0].first()) {
            assertEquals("key1", key())
            assertEquals("value1", value())
            assertEquals(0, offset())
        }

        with(collectedRecords[1].first()) {
            assertEquals("key2", key())
            assertEquals("value2", value())
            assertEquals(1, offset())
        }
    }

    @Test
    fun `should respect poll interval between polls`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollingConfig = PollingConfig(
            pollTimeout = 100.milliseconds,
            pollInterval = 500.milliseconds
        )

        val record = createConsumerRecord("key", "value", 0)
        coEvery { consumer.poll(any()) } returns createConsumerRecords(listOf(record))

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When
        launch {
            poller.observeRecords().take(3).collect()
        }

        // First poll happens immediately
        advanceTimeBy(100)
        coVerify(exactly = 1) { consumer.poll(any()) }

        // Second poll after pollInterval
        advanceTimeBy(500)
        coVerify(exactly = 2) { consumer.poll(any()) }

        // Third poll after another pollInterval
        advanceTimeBy(500)
        coVerify(exactly = 3) { consumer.poll(any()) }
    }

    @Test
    fun `should share polling between multiple collectors`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollingConfig = PollingConfig(
            pollTimeout = 100.milliseconds,
            pollInterval = 100.milliseconds
        )

        val record = createConsumerRecord("key", "value", 0)
        coEvery { consumer.poll(any()) } returns createConsumerRecords(listOf(record))

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When - Two collectors subscribe
        val collector1Records = mutableListOf<ConsumerRecords<String, String>>()
        val collector2Records = mutableListOf<ConsumerRecords<String, String>>()

        launch {
            poller.observeRecords().take(2).toList(collector1Records)
        }

        launch {
            poller.observeRecords().take(2).toList(collector2Records)
        }

        advanceTimeBy(200)

        // Then - Both collectors should receive the same records
        assertEquals(2, collector1Records.size)
        assertEquals(2, collector2Records.size)

        // Same references because it's a SharedFlow
        assertEquals(collector1Records[0], collector2Records[0])
        assertEquals(collector1Records[1], collector2Records[1])

        // Poll should be called 2 times
        coVerify(exactly = 2) { consumer.poll(any()) }
    }

    @Test
    fun `should continue polling after collector is completed`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollingConfig = PollingConfig(
            pollTimeout = 100.milliseconds,
            pollInterval = 100.milliseconds
        )

        val record = createConsumerRecord("key", "value", 0)
        coEvery { consumer.poll(any()) } returns createConsumerRecords(listOf(record))

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When - First collector subscribes and then cancels
        launch {
            poller.observeRecords().take(2).collect()
        }

        advanceTimeBy(300)
        coVerify(exactly = 3) { consumer.poll(any()) }

        // Then - Second collector subscribes and should receive new records
        launch {
            poller.observeRecords().take(1).collect()
        }

        advanceTimeBy(200)

        // Polling continues even after first collector completed
        coVerify(exactly = 5) { consumer.poll(any()) }
    }

    @Test
    fun `should poll continuously while background scope is active`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollingConfig = PollingConfig(
            pollTimeout = 100.milliseconds,
            pollInterval = 100.milliseconds
        )

        val record = createConsumerRecord("key", "value", 0)
        coEvery { consumer.poll(any()) } returns createConsumerRecords(listOf(record))

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When - Collector subscribes
        launch {
            poller.observeRecords().take(3).collect()
        }

        advanceTimeBy(500)

        // Then - Polling should happen multiple times
        coVerify(exactly = 5) { consumer.poll(any()) }

        // When - scope is canceled
        backgroundScope.coroutineContext.cancelChildren()
        advanceTimeBy(1000)

        // Then - Polling has stopped - poll count has not changed
        coVerify(exactly = 5) { consumer.poll(any()) }
    }

    @Test
    fun `should handle poll timeout correctly`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollTimeout = 200.milliseconds
        val pollingConfig = PollingConfig(
            pollTimeout = pollTimeout,
            pollInterval = 100.milliseconds
        )

        coEvery { consumer.poll(pollTimeout) } coAnswers {
            delay(pollTimeout)
            emptyConsumerRecords()
        }

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When
        val job = launch {
            poller.observeRecords().collect()
        }

        advanceTimeBy(300)

        // Then - Should call poll with correct timeout
        coVerify(exactly = 1) { consumer.poll(pollTimeout) }

        job.cancelAndJoin()
    }

    @Test
    fun `should emit records with correct count and data`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollingConfig = PollingConfig(
            pollTimeout = 100.milliseconds,
            pollInterval = 100.milliseconds
        )

        val records = (1..5).map { createConsumerRecord("key$it", "value$it", it.toLong()) }
        coEvery { consumer.poll(any()) } returns createConsumerRecords(records)

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When
        val collectedRecords = mutableListOf<ConsumerRecords<String, String>>()
        launch {
            poller.observeRecords().take(1).toList(collectedRecords)
        }

        advanceTimeBy(200)

        // Then
        assertEquals(1, collectedRecords.size)
        assertEquals(5, collectedRecords[0].count())

        val recordsList = collectedRecords[0].toList()
        (1..5).forEach {
            assertEquals("key$it", recordsList[it - 1].key())
            assertEquals("value$it", recordsList[it - 1].value())
            assertEquals(it.toLong(), recordsList[it - 1].offset())
        }
    }

    @Test
    fun `should handle multiple partitions in single poll`() = runTest {
        // Given
        val consumer = mockk<Consumer<String, String>>()
        val pollingConfig = PollingConfig(
            pollTimeout = 100.milliseconds,
            pollInterval = 100.milliseconds
        )

        val partition0Records = listOf(
            createConsumerRecord("key1", "value1", 0, partition = 0),
            createConsumerRecord("key2", "value2", 1, partition = 0)
        )
        val partition1Records = listOf(
            createConsumerRecord("key3", "value3", 0, partition = 1),
            createConsumerRecord("key4", "value4", 1, partition = 1)
        )

        val allRecords = mapOf(
            0 to partition0Records,
            1 to partition1Records
        )
            .mapKeys { (partitionNum, _) -> TopicPartition("test-topic", partitionNum) }
            .let { ConsumerRecords(it, emptyMap()) }

        coEvery { consumer.poll(any()) } returns allRecords

        val poller = Poller(consumer, pollingConfig, backgroundScope)

        // When
        val collectedRecords = mutableListOf<ConsumerRecords<String, String>>()
        launch {
            poller.observeRecords().take(1).toList(collectedRecords)
        }

        advanceTimeBy(200)

        // Then
        assertEquals(1, collectedRecords.size)
        assertEquals(4, collectedRecords[0].count())
        assertEquals(2, collectedRecords[0].partitions().size)
    }

    // Helper functions

    private fun emptyConsumerRecords(): ConsumerRecords<String, String> {
        return ConsumerRecords(emptyMap(), emptyMap())
    }

    private fun createConsumerRecord(
        key: String,
        value: String,
        offset: Long,
        partition: Int = 0
    ): ConsumerRecord<String, String> {
        return ConsumerRecord(
            "test-topic",
            partition,
            offset,
            key,
            value
        )
    }

    private fun createConsumerRecords(
        records: List<ConsumerRecord<String, String>>
    ): ConsumerRecords<String, String> {
        val partition = TopicPartition("test-topic", 0)
        return ConsumerRecords(mapOf(partition to records), emptyMap())
    }
}
