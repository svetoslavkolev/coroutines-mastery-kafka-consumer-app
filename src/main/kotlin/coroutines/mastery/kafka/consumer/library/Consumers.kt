package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.CoroutineScope

data class ConsumerContext<K, V>(
    val consumer: Consumer<K, V>,
    val poller: Poller<K, V>
)

object Consumers {

    fun <K, V> start(
        kafkaProperties: KafkaProperties,
        topics: List<String>,
        recordProcessor: RecordProcessor<K, V>,
        backgroundScope: CoroutineScope
    ): ConsumerContext<K, V> {
        val consumer = Consumer<K, V>(kafkaProperties, topics, backgroundScope)
        val poller = Poller(consumer, backgroundScope)
        val qMgr = QueueManager(poller, consumer, backgroundScope)
        val executor = Executor(consumer, qMgr, recordProcessor, backgroundScope)
        OffsetManager(consumer, executor, backgroundScope)
        return ConsumerContext(consumer, poller)
    }

}
