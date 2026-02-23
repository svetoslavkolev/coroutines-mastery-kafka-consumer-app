package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import kotlin.concurrent.thread
import kotlin.time.Duration
import kotlin.time.toJavaDuration

sealed interface PartitionsChangedEvent {
    val partitions: Collection<TopicPartition>

    data class PartitionsRevoked(override val partitions: Collection<TopicPartition>) :
        PartitionsChangedEvent

    data class PartitionsAssigned(override val partitions: Collection<TopicPartition>) :
        PartitionsChangedEvent
}

class Consumer<K, V>(
    kafkaProperties: KafkaProperties,
    topics: List<String>,
    private val backgroundScope: CoroutineScope
) {

    private val log = KotlinLogging.logger {}

    private val shutdownHook = thread(start = false) { runBlocking { close() } }

    init {
        Runtime.getRuntime().addShutdownHook(shutdownHook)
    }

    // Since KafkaConsumer is not thread-safe, we need single-threaded dispatcher for all Kafka consumer operations
    private val kafkaDispatcher = Dispatchers.IO.limitedParallelism(1)

    private val kafkaConsumer = KafkaConsumer<K, V>(kafkaProperties.toProps())

    private val partitionsFlow = callbackFlow {
        kafkaConsumer.subscribe(topics.toList(), object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                log.info { "Partitions revoked: $partitions" }
                trySendBlocking(PartitionsChangedEvent.PartitionsRevoked(partitions))
            }

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                log.info { "Partitions assigned: $partitions" }
                trySendBlocking(PartitionsChangedEvent.PartitionsAssigned(partitions))
            }
        })

        awaitClose()
    }
        .onStart { log.info { "Starting consumer for topics $topics..." } }
        .onCompletion { log.info { "Stopped consumer for topics $topics." } }
        .shareIn( // ensure single topic subscription in case of multiple subscribers
            scope = backgroundScope,
            started = SharingStarted.Eagerly
        )

    fun observePartitionsChanges() = partitionsFlow

    suspend fun poll(timeout: Duration): ConsumerRecords<K, V> =
        withContext(kafkaDispatcher) {
            kafkaConsumer.poll(timeout.toJavaDuration())
        }

    suspend fun pause(partitions: Collection<TopicPartition>) {
        if (partitions.isEmpty()) return
        withContext(kafkaDispatcher) {
            kafkaConsumer.pause(partitions)
            log.info { "Paused partitions: $partitions" }
        }
    }

    suspend fun resume(partitions: Collection<TopicPartition>) {
        if (partitions.isEmpty()) return

        withContext(kafkaDispatcher) {
            val partitionsToResume = kafkaConsumer.paused().filter { partitions.contains(it) }
            if (partitionsToResume.isEmpty()) return@withContext

            kafkaConsumer.resume(partitionsToResume)
            log.info { "Resumed partitions: $partitions" }
        }
    }

    suspend fun commitOffsets(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        if (offsets.isEmpty()) return

        withContext(kafkaDispatcher) {
            kafkaConsumer.commitAsync(offsets) { committedOffsets, exception ->
                exception?.let {
                    log.error(it) { "Exception occurred while committing offsets: $offsets" }
                } ?: log.info { "Committed offsets: $committedOffsets" }
            }
        }
    }

    suspend fun commitOffsetsSync(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        if (offsets.isEmpty()) return
        withContext(kafkaDispatcher) {
            kafkaConsumer.commitSync(offsets)
            log.info { "Committed offsets: $offsets" }
        }
    }

    private suspend fun close() {
        log.info { "Cancelling all coroutines..." }
        backgroundScope.coroutineContext.job.cancelAndJoin()
        log.info { "All coroutines cancelled." }

        withContext(kafkaDispatcher) {
            try {
                log.info { "Closing Kafka consumer..." }
                kafkaConsumer.close()
                log.info { "Kafka consumer closed successfully." }
            } catch (e: Exception) {
                currentCoroutineContext().ensureActive()
                log.error(e) { "Error closing Kafka consumer." }
            }
        }
    }
}

