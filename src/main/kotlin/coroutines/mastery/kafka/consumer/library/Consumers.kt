package coroutines.mastery.kafka.consumer.library

import coroutines.mastery.kafka.consumer.library.config.ConsumerConfig
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import mu.KotlinLogging

data class ConsumerHandle<K, V>(
    val rebalances: RebalanceFlow,
    val records: RecordsFlow<K, V>
)

object Consumers {

    private val log = KotlinLogging.logger {}

    fun <K, V> start(consumerConfig: ConsumerConfig<K, V>): ConsumerHandle<K, V> {
        val backgroundScope =
            CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, throwable ->
                log.error(throwable) { "Exception occurred in a coroutine: ${throwable.message}" }
            })

        val consumer = Consumer(consumerConfig, backgroundScope)
        val poller = Poller(consumer, consumerConfig.polling, backgroundScope)
        val qMgr = QueueManager(
            poller, consumer, consumerConfig.recordProcessing, backgroundScope
        )
        val executor = Executor(
            consumer, qMgr, consumerConfig.recordProcessing, backgroundScope
        )
        OffsetManager(consumer, executor, consumerConfig.offsetHandling, backgroundScope)
        return ConsumerHandle(consumer, poller)
    }

}
