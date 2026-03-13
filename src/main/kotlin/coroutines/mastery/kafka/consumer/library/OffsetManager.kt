package coroutines.mastery.kafka.consumer.library

import coroutines.mastery.kafka.consumer.library.config.OffsetHandlingConfig
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.RebalanceInProgressException
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class OffsetManager<K, V>(
    private val consumer: Consumer<K, V>,
    private val executor: Executor<K, V>,
    private val offsetHandlingConfig: OffsetHandlingConfig,
    private val backgroundScope: CoroutineScope,
) {

    private val log = KotlinLogging.logger {}
    private val nextOffsets = ConcurrentHashMap<TopicPartition, OffsetAndMetadata>()
    private val mutex = Mutex()

    init {
        executor.registerOffsetNotifier { event ->
            mutex.withLock {
                nextOffsets[event.partition] = OffsetAndMetadata(
                    event.latestOffset + 1, // offset of the next record should be committed
                    Optional.ofNullable(event.leaderEpoch),
                    ""
                )
            }
        }

        registerRebalanceCallback()
        launchPeriodicOffsetCommit()
    }

    private fun registerRebalanceCallback() {
        consumer.registerRebalanceCallback(object : RebalanceCallback {

            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                runBlocking {
                    // waiting for executor jobs to complete before committing offsets for revoked partitions
                    // as those executor jobs will be canceled on partition revocation and
                    // will notify about their last processed offsets, therefore, waiting for their completion
                    // to be sure the offsets were emitted before committing them to Kafka
                    executor.awaitPartitionJobs(partitions)

                    mutex.withLock {
                        val offsetsToCommit = nextOffsets.filterKeys { it in partitions }
                        log.info { "Committing offsets due to partition revocation: $offsetsToCommit" }

                        // A non-suspending variant of offset commits needs to be used because
                        // we're already on the kafkaDispatcher thread and usage of withContext(kafkaDispatcher)
                        // will suspend forever (this callback is invoked from onPartitionsRevoked
                        // which is called from poll() which is running on kafkaDispatcher's thread).
                        consumer.commitOffsetsBlocking(offsetsToCommit)
                        offsetsToCommit.keys.forEach { nextOffsets.remove(it) }
                    }
                }
            }

            override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
                runBlocking {
                    // waiting for executor jobs to complete as they may emit their last offsets,
                    // and we need to make sure they've completed to be able to clean up
                    // those offsets from the map, so to avoid committing them as commit is not possible
                    // when partitions are lost.
                    executor.awaitPartitionJobs(partitions)

                    mutex.withLock {
                        val offsetsToCleanup = nextOffsets.filterKeys { it in partitions }
                        log.info { "Cleanup offsets for lost partitions: $offsetsToCleanup" }
                        offsetsToCleanup.keys.forEach { nextOffsets.remove(it) }
                    }
                }
            }
        })
    }

    private fun launchPeriodicOffsetCommit() {
        backgroundScope.launch {
            log.info { "Launched periodic offset commit job with interval ${offsetHandlingConfig.offsetCommitInterval}." }
            while (currentCoroutineContext().isActive) {
                delay(offsetHandlingConfig.offsetCommitInterval)

                val offsetsToCommit = mutex.withLock { nextOffsets.toMap() }
                log.info { "Committing offsets $offsetsToCommit" }

                try {
                    consumer.commitOffsets(offsetsToCommit)
                    mutex.withLock {
                        offsetsToCommit.keys
                            .filter { partition ->
                                offsetsToCommit[partition]?.offset() == nextOffsets[partition]?.offset()
                            }
                            .forEach { nextOffsets.remove(it) }
                    }
                } catch (e: CommitFailedException) {
                    log.error(e) { "CommitFailedException while committing offsets: ${e.message}." }
                    mutex.withLock { nextOffsets.clear() }
                } catch (e: RebalanceInProgressException) {
                    log.error(e) { "RebalanceInProgressException while committing offsets: ${e.message}." }
                    mutex.withLock { nextOffsets.clear() }
                } catch (e: Exception) {
                    currentCoroutineContext().ensureActive()
                    log.error(e) { "Error committing offsets, will be retried: ${e.message}." }
                }
            }
        }
    }

}
