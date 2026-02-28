package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ConcurrentHashMap

fun interface RecordProcessor<K, V> {
    suspend fun process(record: ConsumerRecord<K, V>)
}

fun interface OffsetNotifier {
    suspend fun notifyLatestOffset(offset: LatestProcessedOffset)
}

data class LatestProcessedOffset(
    val partition: TopicPartition,
    val latestOffset: Long,
    val leaderEpoch: Int?
)

class Executor<K, V>(
    private val consumer: Consumer<K, V>,
    private val queueManager: QueueManager<K, V>,
    private val recordProcessor: RecordProcessor<K, V>,
    private val backgroundScope: CoroutineScope,
    private val dispatcher: CoroutineDispatcher = Dispatchers.IO.limitedParallelism(5),
) {

    private val log = KotlinLogging.logger {}
    private val partitionJobs = ConcurrentHashMap<TopicPartition, Job>()
    private val offsetNotifiers = ConcurrentHashMap.newKeySet<OffsetNotifier>()

    init {
        observeQueues()
    }

    fun registerOffsetNotifier(offsetNotifier: OffsetNotifier) {
        offsetNotifiers.add(offsetNotifier)
    }

    suspend fun awaitPartitionJobs(partitions: Collection<TopicPartition>) {
        partitions.forEachAsync { partition ->
            partitionJobs[partition]?.let { job ->
                log.info { "Waiting for executor job for partition $partition to complete..." }
                job.join()
                log.info { "Executor job for partition $partition completed" }
            }
        }
    }

    suspend fun awaitAllPartitionJobs() {
        val jobs = partitionJobs.values
        if (jobs.isNotEmpty()) {
            log.info { "Waiting for all ${jobs.size} executor jobs to complete..." }
            jobs.joinAll()
            log.info { "All executor jobs completed" }
        }
    }

    private fun observeQueues() {
        queueManager.observeQueueLifecycle()
            .onEach { event ->
                when (event) {
                    is QueueLifecycleEvent.QueueCreated -> {
                        val job = backgroundScope.launch(dispatcher) {
                            processPartition(event.partition, event.queue)
                        }
                        partitionJobs[event.partition] = job
                    }

                    is QueueLifecycleEvent.QueueRemoved -> {
                        partitionJobs.remove(event.partition)?.cancel()
                        log.info { "Cancelled executor job for partition ${event.partition}" }
                    }
                }
            }
            .onStart { log.info { "Start collecting queue lifecycle..." } }
            .onCompletion { log.info { "Stopped collecting queue lifecycle." } }
            .launchIn(backgroundScope)
    }

    private suspend fun processPartition(
        partition: TopicPartition,
        queue: ReceiveChannel<List<ConsumerRecord<K, V>>>
    ) {
        log.info { "Started executor job for partition $partition" }

        var lastProcessedOffset: Long? = null
        var lastLeaderEpoch: Int? = null

        for (records in queue) {
            log.info { "Processing ${records.size} records from partition $partition ..." }

            var processedInBatch = 0
            try {
                records.forEach { record ->
                    recordProcessor.process(record)
                    lastProcessedOffset = record.offset()
                    lastLeaderEpoch = record.leaderEpoch().orElse(null)
                    processedInBatch++
                }

                @OptIn(ExperimentalCoroutinesApi::class)
                if (queue.isEmpty) {
                    // resume a partition after the channel is empty and NOT on every processed batch
                    // to avoid too frequent pause/resume cycles
                    consumer.resume(listOf(partition))
                }
            } finally {
                lastProcessedOffset?.let { offset ->
                    val message = "Processed $processedInBatch/${records.size} records " +
                            "from partition $partition, " +
                            "last processed offset: $offset"
                    if (processedInBatch < records.size) log.warn(message) else log.info(message)

                    val latestOffset = LatestProcessedOffset(
                        partition = partition,
                        latestOffset = offset,
                        leaderEpoch = lastLeaderEpoch
                    )

                    // using a custom notifier to notify about the latest processed offset
                    // SYNCHRONOUSLY. This is especially important for shutdown
                    // and partition revocation scenarios where offsets of partially processed
                    // batches still need to be committed to Kafka in order to avoid
                    // reconsumption of the same records by another consumer. E.g. if the
                    // coroutine was currently processing a batch of records with offsets
                    // between 5000 and 5500 and was canceled while processing record
                    // with offset 5450, then this synchronous notification guarantees that
                    // the offset 5449 will be committed and only the record at offset 5450
                    // will be redelivered to the new consumer after next assignment of partitions.
                    // SharedFlow can't be used here as only emitting is guaranteed,
                    // not the collection (the coroutine that would collect from this SharedFlow
                    // may be canceled before the executor job is canceled).
                    withContext(NonCancellable) {
                        offsetNotifiers.forEach {
                            it.notifyLatestOffset(latestOffset)
                        }
                    }
                } ?: log.warn { "No records were processed from partition $partition" }
            }
        }
    }
}
