package coroutines.mastery.kafka.consumer.library

import coroutines.mastery.kafka.consumer.library.config.ConsumerConfig
import coroutines.mastery.kafka.consumer.library.config.toKafkaProperties
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
import org.apache.kafka.common.errors.InterruptException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.concurrent.thread
import kotlin.time.Duration
import kotlin.time.toJavaDuration

sealed interface RebalanceEvent {
    val partitions: Collection<TopicPartition>

    @JvmInline
    value class PartitionsAssigned(override val partitions: Collection<TopicPartition>) :
        RebalanceEvent

    @JvmInline
    value class PartitionsRevoked(override val partitions: Collection<TopicPartition>) :
        RebalanceEvent

    @JvmInline
    value class PartitionsLost(override val partitions: Collection<TopicPartition>) :
        RebalanceEvent
}

interface RebalanceFlow {
    fun observeRebalanceEvents(): Flow<RebalanceEvent>
}

/**
 * A synchronous callback to be invoked from ConsumerRebalanceListener methods,
 * as some operations, e.g. offset commit for revoked partitions, need to be called from within the
 * ConsumerRebalanceListener callbacks. The current architecture with asynchronous flow processing
 * is not appropriate for each and every operation related to Kafka. That's why this interface exists.
 */
interface RebalanceCallback {
    fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {}
    fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {}
    fun onPartitionsLost(partitions: Collection<TopicPartition>) {}
}

@OptIn(ExperimentalAtomicApi::class)
class Consumer<K, V>(
    config: ConsumerConfig<K, V>,
    private val backgroundScope: CoroutineScope
) : RebalanceFlow {

    private val log = KotlinLogging.logger {}

    private val executor = Executors.newSingleThreadExecutor()

    private val wasShutdown = AtomicBoolean(false)

    private val shutdownTask = Runnable {
        if (wasShutdown.compareAndSet(expectedValue = false, newValue = true)) {
            log.info { "Kafka Consumer shutdown initiated. Cleaning up resources..." }
            runBlocking { close() }
            executor.shutdown()
            log.info { "Kafka Consumer shutdown completed. All resources cleaned up." }
        }
    }

    init {
        val shutdownHook = thread(start = false) {
            shutdownTask.run()
        }
        Runtime.getRuntime().addShutdownHook(shutdownHook)
    }

    // Since KafkaConsumer is not thread-safe, we need single-threaded dispatcher for all Kafka consumer operations
    private val kafkaDispatcher = Dispatchers.IO.limitedParallelism(1)

    private val kafkaConsumer = KafkaConsumer<K, V>(config.toKafkaProperties())

    private var rebalanceCallbacks = ConcurrentHashMap.newKeySet<RebalanceCallback>()

    private val rebalanceFlow = callbackFlow {
        val rebalanceListener = object : ConsumerRebalanceListener {

            override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                log.info { "Partitions assigned: $partitions" }
                trySendBlocking(RebalanceEvent.PartitionsAssigned(partitions))
                rebalanceCallbacks.forEach { it.onPartitionsAssigned(partitions) }
            }

            override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                log.info { "Partitions revoked: $partitions" }
                trySendBlocking(RebalanceEvent.PartitionsRevoked(partitions))
                rebalanceCallbacks.forEach { it.onPartitionsRevoked(partitions) }
            }

            override fun onPartitionsLost(partitions: Collection<TopicPartition>) {
                log.warn { "Partitions lost (consumer kicked out of the group): $partitions" }
                trySendBlocking(RebalanceEvent.PartitionsLost(partitions))
                rebalanceCallbacks.forEach { it.onPartitionsLost(partitions) }
            }
        }

        kafkaConsumer.subscribe(config.topics.toList(), rebalanceListener)
        awaitClose()
    }
        .onStart { log.info { "Starting consumer for topics ${config.topics}..." } }
        .onCompletion { log.info { "Stopped consumer for topics ${config.topics}." } }
        .shareIn( // ensure single topic subscription in case of multiple subscribers
            scope = backgroundScope,
            started = SharingStarted.Eagerly
        )

    override fun observeRebalanceEvents(): Flow<RebalanceEvent> = rebalanceFlow

    fun registerRebalanceCallback(callback: RebalanceCallback) {
        rebalanceCallbacks.add(callback)
    }

    suspend fun poll(timeout: Duration): ConsumerRecords<K, V> =
        withContext(kafkaDispatcher) {
            try {
                kafkaConsumer.poll(timeout.toJavaDuration())
            } catch (e: InterruptException) {
                log.info { "Consumer interrupted, initiating immediate shutdown..." }
                executor.execute(shutdownTask)
                throw e
            } catch (e: Exception) {
                currentCoroutineContext().ensureActive()
                log.error(e) { "Fatal error during polling, initiating immediate shutdown because: ${e.message}" }
                executor.execute(shutdownTask)
                throw e
            }
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
                    log.error(it) { "Exception occurred while committing offsets: $committedOffsets" }
                } ?: log.info { "Committed offsets: $committedOffsets" }
            }
        }
    }

    fun commitOffsetsBlocking(offsets: Map<TopicPartition, OffsetAndMetadata>) {
        if (offsets.isEmpty()) return
        kafkaConsumer.commitSync(offsets)
        log.info { "Committed offsets: $offsets" }
    }

    private suspend fun close() {
        log.info { "Cancelling all coroutines..." }
        // no need to keep the scope active as we are in shutdown phase
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

