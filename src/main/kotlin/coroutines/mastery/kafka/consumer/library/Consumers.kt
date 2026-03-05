package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import mu.KotlinLogging

data class ConsumerContext<K, V>(
    val consumer: Consumer<K, V>,
    val poller: Poller<K, V>
)

object Consumers {

    private val log = KotlinLogging.logger {}

    fun <K, V> start(
        kafkaProperties: KafkaProperties,
        topics: List<String>,
        recordProcessor: RecordProcessor<K, V>
    ): ConsumerContext<K, V> {
        val backgroundScope =
            CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, throwable ->
                log.error(throwable) { "Exception occurred in a coroutine: ${throwable.message}" }
            })

        val consumer = Consumer<K, V>(kafkaProperties, topics, backgroundScope)
        val poller = Poller(consumer, backgroundScope)
        val qMgr = QueueManager(poller, consumer, backgroundScope)
        val executor = Executor(consumer, qMgr, recordProcessor, backgroundScope)
        OffsetManager(consumer, executor, backgroundScope)
        return ConsumerContext(consumer, poller)
    }

}
