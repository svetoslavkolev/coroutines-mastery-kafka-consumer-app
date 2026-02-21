package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ConcurrentHashMap

fun interface RecordProcessor<K, V> {
    suspend fun process(record: ConsumerRecord<K, V>)
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
    private val latestOffsetsFlow = MutableSharedFlow<LatestProcessedOffset>()

    init {
        observeQueues()
    }

    fun observeLatestOffsets() = latestOffsetsFlow.asSharedFlow()

    private fun observeQueues() {
        queueManager.observeQueueLifecycle()
            .onEach { event ->
                when (event) {
                    is QueueLifecycleEvent.QueueCreated -> {
                        val job = backgroundScope.launch(dispatcher) {
                            processPartition(event.partition, event.queue)
                        }
                        partitionJobs[event.partition] = job
                        log.info { "Started executor job for partition ${event.partition}" }
                    }

                    is QueueLifecycleEvent.QueueRemoved -> {
                        partitionJobs.remove(event.partition)?.cancel()
                        log.info { "Cancelled executor job for partition ${event.partition}" }
                    }
                }
            }
            .launchIn(backgroundScope)
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun processPartition(
        partition: TopicPartition,
        queue: ReceiveChannel<List<ConsumerRecord<K, V>>>
    ) {
        for (records in queue) {
            log.info { "Processing ${records.size} records from partition $partition ..." }
            records.forEach { recordProcessor.process(it) }
            log.info { "Done processing ${records.size} records from partition $partition" }

            records.maxByOrNull { it.offset() }?.let { latestRecord ->
                latestOffsetsFlow.emit(
                    LatestProcessedOffset(
                        partition = partition,
                        latestOffset = latestRecord.offset(),
                        leaderEpoch = latestRecord.leaderEpoch().orElse(null)
                    )
                )
            }

            if (queue.isEmpty) {
                // resume a partition after the channel is empty and NOT on every processed batch
                // to avoid too frequent pause/resume cycles
                consumer.resume(listOf(partition))
            }
        }
    }
}
