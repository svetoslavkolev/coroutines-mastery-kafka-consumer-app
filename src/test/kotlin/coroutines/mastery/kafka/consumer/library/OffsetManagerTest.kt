package coroutines.mastery.kafka.consumer.library

import coroutines.mastery.kafka.consumer.library.config.OffsetHandlingConfig
import io.mockk.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RebalanceInProgressException
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class OffsetManagerTest {

    private lateinit var consumer: Consumer<String, String>
    private lateinit var executor: Executor<String, String>
    private lateinit var offsetHandlingConfig: OffsetHandlingConfig

    @BeforeTest
    fun setup() {
        consumer = mockk<Consumer<String, String>>()
        executor = mockk<Executor<String, String>>()
        offsetHandlingConfig = OffsetHandlingConfig(offsetCommitInterval = 100.milliseconds)

        coEvery { consumer.commitOffsets(any()) } just Runs
        every { consumer.commitOffsetsBlocking(any()) } just Runs
        every { consumer.registerRebalanceCallback(any()) } just Runs
        every { executor.registerOffsetNotifier(any()) } just Runs
        coEvery { executor.awaitPartitionJobs(any()) } just Runs
    }

    @Test
    fun `should register offset notifier with executor`() = runTest {
        // When
        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // Then
        verify(exactly = 1) { executor.registerOffsetNotifier(any()) }
    }

    @Test
    fun `should register rebalance callback with consumer`() = runTest {
        // When
        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // Then
        verify(exactly = 1) { consumer.registerRebalanceCallback(any()) }
    }

    @Test
    fun `should track offsets when notifier is called`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Notify about processed offset
        capturedNotifier?.notifyLatestOffset(
            LatestProcessedOffset(partition, 100, 5)
        )
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should commit offset 101 (next offset after 100)
        coVerify(exactly = 1) {
            consumer.commitOffsets(
                match { offsets ->
                    offsets[partition]?.offset() == 101L &&
                            offsets[partition]?.leaderEpoch()?.get() == 5
                }
            )
        }
    }

    @Test
    fun `should commit offsets periodically`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Notify about multiple offsets and wait for periodic commits
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 200, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 300, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should have committed 3 times in order with correct offsets
        coVerifyOrder {
            consumer.commitOffsets(match { it[partition]?.offset() == 101L })
            consumer.commitOffsets(match { it[partition]?.offset() == 201L })
            consumer.commitOffsets(match { it[partition]?.offset() == 301L })
        }
    }

    @Test
    fun `should commit offsets for revoked partitions synchronously`() = runTest {
        // Given
        val partition0 = TopicPartition("test-topic", 0)
        val partition1 = TopicPartition("test-topic", 1)

        var capturedCallback: RebalanceCallback? = null
        var capturedNotifier: OffsetNotifier? = null

        every { consumer.registerRebalanceCallback(any()) } answers {
            capturedCallback = firstArg()
        }

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // Setup - Track offsets for both partitions
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition0, 100, 5))
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition1, 200, 5))

        // When - Revoke partition0
        capturedCallback?.onPartitionsRevoked(listOf(partition0))

        // Then - Should await jobs and commit only revoked partition's offset
        coVerify(exactly = 1) { executor.awaitPartitionJobs(listOf(partition0)) }
        verify(exactly = 1) {
            consumer.commitOffsetsBlocking(
                match { offsets ->
                    offsets.size == 1 &&
                            offsets[partition0]?.offset() == 101L &&
                            offsets[partition1] == null
                }
            )
        }
    }

    @Test
    fun `should cleanup offsets for lost partitions without committing`() = runTest {
        // Given
        val partition0 = TopicPartition("test-topic", 0)
        val partition1 = TopicPartition("test-topic", 1)

        var capturedCallback: RebalanceCallback? = null
        var capturedNotifier: OffsetNotifier? = null

        every { consumer.registerRebalanceCallback(any()) } answers {
            capturedCallback = firstArg()
        }

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // Setup - Track offsets for both partitions
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition0, 100, 5))
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition1, 200, 5))

        // When - Lose partition0
        capturedCallback?.onPartitionsLost(listOf(partition0))

        // Then - Should await jobs but NOT commit offsets for lost partition
        coVerify(exactly = 1) { executor.awaitPartitionJobs(listOf(partition0)) }
        verify(exactly = 0) { consumer.commitOffsetsBlocking(any()) }

        // Verify periodic commit only commits partition1 offset after partition0 was lost
        advanceTimeBy(100.milliseconds)
        runCurrent()

        coVerify(exactly = 1) {
            consumer.commitOffsets(
                match { offsets ->
                    offsets.size == 1 &&
                            offsets[partition1]?.offset() == 201L &&
                            offsets[partition0] == null
                }
            )
        }
    }

    @Test
    fun `should handle CommitFailedException gracefully in periodic commit`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        coEvery { consumer.commitOffsets(any()) } throws CommitFailedException(
            "Consumer was kicked out of the group"
        )

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Try to commit offsets
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should catch exception and continue
        coVerify(exactly = 1) { consumer.commitOffsets(any()) }

        // Verify periodic job continues after exception
        advanceTimeBy(100.milliseconds)
        runCurrent()

        coVerify(exactly = 2) { consumer.commitOffsets(any()) }
    }

    @Test
    fun `should handle RebalanceInProgressException gracefully in periodic commit`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        coEvery { consumer.commitOffsets(any()) } throws RebalanceInProgressException(
            "Rebalance in progress"
        )

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Try to commit offsets
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should catch exception and continue
        coVerify(exactly = 1) { consumer.commitOffsets(any()) }

        // Verify periodic job continues after exception
        advanceTimeBy(100.milliseconds)
        runCurrent()

        coVerify(exactly = 2) { consumer.commitOffsets(any()) }
    }

    @Test
    fun `should handle generic exceptions in periodic commit and retry`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        var callCount = 0
        coEvery { consumer.commitOffsets(any()) } answers {
            if (callCount++ == 0) {
                throw RuntimeException("Network error")
            }
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - First commit fails, second succeeds
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should have tried once and failed
        coVerify(exactly = 1) { consumer.commitOffsets(any()) }

        // When - Retry in next cycle
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should succeed on retry
        coVerify(exactly = 2) {
            consumer.commitOffsets(
                match { offsets -> offsets[partition]?.offset() == 101L }
            )
        }
    }

    @Test
    fun `should not commit already committed offsets`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Notify and commit
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - First commit happens with non-empty map
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { offsets ->
                offsets.isNotEmpty() && offsets[partition]?.offset() == 101L
            })
        }

        // When - Wait for next cycle without new offsets
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should have 2 total calls, second one with empty map
        coVerify(exactly = 2) { consumer.commitOffsets(any()) }
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it.isEmpty() })
        }
    }

    @Test
    fun `should update offset for same partition`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Notify with offset 100
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should commit 101
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it[partition]?.offset() == 101L })
        }

        // When - Update to offset 200
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 200, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should commit 201
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it[partition]?.offset() == 201L })
        }
    }

    @Test
    fun `should handle multiple partitions independently`() = runTest {
        // Given
        val partition0 = TopicPartition("test-topic", 0)
        val partition1 = TopicPartition("test-topic", 1)
        val partition2 = TopicPartition("test-topic", 2)

        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Notify different offsets for different partitions
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition0, 100, 5))
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition1, 200, 6))
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition2, 300, 7))

        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should commit all three partitions with correct offsets
        coVerify(exactly = 1) {
            consumer.commitOffsets(
                match { offsets ->
                    offsets.size == 3 &&
                            offsets[partition0]?.offset() == 101L &&
                            offsets[partition1]?.offset() == 201L &&
                            offsets[partition2]?.offset() == 301L
                }
            )
        }
    }

    @Test
    fun `should not remove offset if it was updated after periodic commit started`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        // Simulate slow commit that allows offset update during commit
        coEvery { consumer.commitOffsets(any()) } coAnswers {
            delay(50.milliseconds)
            capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 200, 5))
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Notify with offset 100
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))
        advanceTimeBy(150.milliseconds) // Allow commit to complete
        runCurrent()

        // Then - Offset 101 should be committed, but offset 201 should remain in map
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it[partition]?.offset() == 101L })
        }

        // When - Next periodic cycle
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should commit the updated offset 201
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it[partition]?.offset() == 201L })
        }
    }

    @Test
    fun `should await executor jobs before committing on partition revocation`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedCallback: RebalanceCallback? = null
        var capturedNotifier: OffsetNotifier? = null

        every { consumer.registerRebalanceCallback(any()) } answers {
            capturedCallback = firstArg()
        }

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // Setup offset
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))

        // When - Revoke partition
        capturedCallback?.onPartitionsRevoked(listOf(partition))

        // Then - Should await before committing
        coVerifyOrder {
            executor.awaitPartitionJobs(listOf(partition))
            consumer.commitOffsetsBlocking(match { it[partition]?.offset() == 101L })
        }
    }

    @Test
    fun `should await executor jobs before cleanup on partition lost`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedCallback: RebalanceCallback? = null
        var capturedNotifier: OffsetNotifier? = null

        every { consumer.registerRebalanceCallback(any()) } answers {
            capturedCallback = firstArg()
        }

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // Setup offset
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))

        // When - Lose partition
        capturedCallback?.onPartitionsLost(listOf(partition))

        // Then - Should await jobs
        coVerify(exactly = 1) { executor.awaitPartitionJobs(listOf(partition)) }
        // Should NOT commit
        verify(exactly = 0) { consumer.commitOffsetsBlocking(any()) }
    }

    @Test
    fun `should remove committed offset from map after successful commit`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Notify and commit
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should commit offset 101
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it[partition]?.offset() == 101L })
        }

        // When - Next cycle without new updates
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should not commit the same offset again (map should be empty)
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it.isEmpty() })
        }
    }

    @Test
    fun `should handle offset with null leader epoch`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedNotifier: OffsetNotifier? = null

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // When - Notify with null leader epoch
        capturedNotifier?.notifyLatestOffset(
            LatestProcessedOffset(partition, 100, null)
        )
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should commit offset 101 with empty optional leader epoch
        coVerify(exactly = 1) {
            consumer.commitOffsets(
                match { offsets ->
                    offsets[partition]?.offset() == 101L &&
                            offsets[partition]?.leaderEpoch()?.isEmpty == true
                }
            )
        }
    }

    @Test
    fun `should remove offset for revoked partition after commit`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedCallback: RebalanceCallback? = null
        var capturedNotifier: OffsetNotifier? = null

        every { consumer.registerRebalanceCallback(any()) } answers {
            capturedCallback = firstArg()
        }

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // Setup - Track offset
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))

        // When - Revoke partition
        capturedCallback?.onPartitionsRevoked(listOf(partition))

        // Then - Should commit
        verify(exactly = 1) {
            consumer.commitOffsetsBlocking(match { it[partition]?.offset() == 101L })
        }

        // When - Next periodic cycle
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should not commit again (offset was removed)
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it.isEmpty() })
        }
    }

    @Test
    fun `should remove offset for lost partition after cleanup`() = runTest {
        // Given
        val partition = TopicPartition("test-topic", 0)
        var capturedCallback: RebalanceCallback? = null
        var capturedNotifier: OffsetNotifier? = null

        every { consumer.registerRebalanceCallback(any()) } answers {
            capturedCallback = firstArg()
        }

        every { executor.registerOffsetNotifier(any()) } answers {
            capturedNotifier = firstArg()
        }

        OffsetManager(consumer, executor, offsetHandlingConfig, backgroundScope)

        // Setup - Track offset
        capturedNotifier?.notifyLatestOffset(LatestProcessedOffset(partition, 100, 5))

        // When - Lose partition
        capturedCallback?.onPartitionsLost(listOf(partition))

        // Then - Should NOT commit (partitions are lost)
        verify(exactly = 0) {
            consumer.commitOffsetsBlocking(any())
        }

        // When - Next periodic cycle
        advanceTimeBy(100.milliseconds)
        runCurrent()

        // Then - Should not commit lost partition's offset (was cleaned up)
        coVerify(exactly = 1) {
            consumer.commitOffsets(match { it.isEmpty() })
        }
    }
}
