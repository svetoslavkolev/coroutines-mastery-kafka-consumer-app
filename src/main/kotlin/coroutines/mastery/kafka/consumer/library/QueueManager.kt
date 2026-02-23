package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.onFailure
import kotlinx.coroutines.channels.onSuccess
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.ConcurrentHashMap

sealed interface QueueLifecycleEvent<K, V> {
    val partition: TopicPartition

    data class QueueCreated<K, V>(
        override val partition: TopicPartition,
        val queue: ReceiveChannel<List<ConsumerRecord<K, V>>>
    ) : QueueLifecycleEvent<K, V>

    data class QueueRemoved<K, V>(override val partition: TopicPartition) :
        QueueLifecycleEvent<K, V>
}

class QueueManager<K, V>(
    private val poller: Poller<K, V>,
    private val consumer: Consumer<K, V>,
    private val backgroundScope: CoroutineScope
) {

    private val log = KotlinLogging.logger {}

    private val queues = ConcurrentHashMap<TopicPartition, Channel<List<ConsumerRecord<K, V>>>>()

    private val queueLifecycleFlow = MutableSharedFlow<QueueLifecycleEvent<K, V>>()

    init {
        observePartitions()
        observeRecords()
    }

    fun observeQueueLifecycle() = queueLifecycleFlow.asSharedFlow()

    private fun observePartitions() {
        consumer.observePartitionsChanges()
            .onEach { event ->
                when (event) {
                    is PartitionsChangedEvent.PartitionsRevoked -> {
                        event.partitions.forEachAsync { partition ->
                            queues.remove(partition)?.cancel()
                            log.info { "Closed queue for revoked partition $partition" }
                            queueLifecycleFlow.emit(QueueLifecycleEvent.QueueRemoved(partition))
                        }
                    }

                    is PartitionsChangedEvent.PartitionsAssigned -> {
                        event.partitions.forEachAsync { partition ->
                            queues[partition] = Channel(capacity = 10)
                            log.info { "Created queue for assigned partition $partition" }
                            queueLifecycleFlow.emit(
                                QueueLifecycleEvent.QueueCreated(partition, queues[partition]!!)
                            )
                        }
                    }
                }
            }
            .onStart { log.info { "Start collecting partition changes..." } }
            .onCompletion {
                queues.values.forEach { it.cancel() }
                log.info { "Stopped collecting partition changes. All partition queues cancelled." }
            }
            .launchIn(backgroundScope)
    }

    private fun observeRecords() {
        poller.observeRecords()
            .onEach { records ->
                val partitionsToPause = ConcurrentHashMap.newKeySet<TopicPartition>()

                records.partitions().forEachAsync { partition ->
                    val partitionRecords = records.records(partition)
                    val offsetsLogMessage =
                        "offsets: min = ${partitionRecords.minOfOrNull { it.offset() }}, " +
                                "max = ${partitionRecords.maxOfOrNull { it.offset() }}"

                    queues[partition]?.trySend(partitionRecords)
                        ?.onSuccess {
                            log.info {
                                "Sent ${partitionRecords.size} records to queue for partition $partition, $offsetsLogMessage"
                            }
                        }
                        ?.onFailure {
                            log.info {
                                "Rejected ${partitionRecords.size} records for partition $partition - queue is full, $offsetsLogMessage"
                            }

                            partitionsToPause.add(partition)
                            resendToQueue(partition, partitionRecords, offsetsLogMessage)
                        }
                        ?: log.error { "Received records from not assigned partition $partition." }
                }

                consumer.pause(partitionsToPause)
            }
            .onStart { log.info { "Start collecting consumer records..." } }
            .onCompletion { log.info { "Stopped collecting consumer records." } }
            .launchIn(backgroundScope)
    }

    private fun resendToQueue(
        partition: TopicPartition,
        rejectedRecords: List<ConsumerRecord<K, V>>,
        offsetsLogMessage: String
    ) {
        // since the rejected records won't be resent from Kafka on resuming
        // the paused partitions,
        // those records need to be retried. And it is done in a
        // separate coroutine to avoid buffer overflow in Poller#shareIn's buffer
        backgroundScope.launch {
            queues[partition]?.send(rejectedRecords)
            log.info {
                "Sent ${rejectedRecords.size} retried records to queue for partition $partition, $offsetsLogMessage"
            }
        }
    }
}
