package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class OffsetManager<K, V>(
    private val consumer: Consumer<K, V>,
    private val executor: Executor<K, V>,
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

        observePartitionRevocations()
        launchPeriodicOffsetCommit()
    }

    private fun observePartitionRevocations() {
        consumer.observePartitionsChanges()
            .filterIsInstance(PartitionsChangedEvent.PartitionsRevoked::class)
            .onEach { revokedEvent ->
                // waiting for executor jobs to complete before committing offsets for revoked partitions
                // as those executor jobs will be canceled on partition revocation and
                // will notify about their last processed offsets, therefore, waiting for their completion
                // to be sure the offsets were emitted before committing them to Kafka
                executor.awaitPartitionJobs(revokedEvent.partitions)

                mutex.withLock {
                    val offsetsToCommit = nextOffsets.filterKeys { it in revokedEvent.partitions }
                    log.info { "Committing offsets due to partition revocation: $offsetsToCommit" }
                    consumer.commitOffsetsSync(offsetsToCommit)
                    offsetsToCommit.keys.forEach { nextOffsets.remove(it) }
                }
            }
            .onStart { log.info { "Start observing partition revocation for offset commits..." } }
            .onCompletion { log.info { "Stopped observing partition revocation for offset commits." } }
            .launchIn(backgroundScope)
    }

    private fun launchPeriodicOffsetCommit() {
        backgroundScope.launch {
            try {
                while (true) {
                    delay(500000)
                    mutex.withLock {
                        log.info { "Committing offsets $nextOffsets" }
                        consumer.commitOffsets(nextOffsets)
                        nextOffsets.clear()
                    }
                }
            } finally {
                withContext(NonCancellable) {
                    // waiting all executor jobs to complete before committing offsets
                    // as those executor jobs will be canceled on shutdown and
                    // will notify about their last processed offsets, therefore, waiting for their completion
                    // to be sure the offsets were emitted before committing them to Kafka
                    executor.awaitAllPartitionJobs()

                    mutex.withLock {
                        log.info { "Committing offsets before shutdown: $nextOffsets" }
                        consumer.commitOffsetsSync(nextOffsets)
                        nextOffsets.clear()
                    }
                }
            }
        }
    }

}
