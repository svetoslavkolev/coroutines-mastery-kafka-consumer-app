package coroutines.mastery.kafka.consumer.library.builder

import coroutines.mastery.kafka.consumer.library.ConsumerHandle
import coroutines.mastery.kafka.consumer.library.Consumers
import coroutines.mastery.kafka.consumer.library.RecordProcessor
import coroutines.mastery.kafka.consumer.library.config.ConsumerConfig
import coroutines.mastery.kafka.consumer.library.config.OffsetHandlingConfig
import coroutines.mastery.kafka.consumer.library.config.PollingConfig
import coroutines.mastery.kafka.consumer.library.config.RecordProcessingConfig
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

fun <K, V> kafkaConsumer(block: ConsumerConfigBuilder<K, V>.() -> Unit): ConsumerHandle<K, V> =
    ConsumerConfigBuilder<K, V>().apply(block).build().let(Consumers::start)

class ConsumerConfigBuilder<K, V> {
    private lateinit var bootstrapServers: String
    private lateinit var consumerGroup: String
    private var topics: List<String> = emptyList()
    private var keyDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class
    private var valueDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class
    private var polling: PollingConfig = PollingConfig()
    private var recordProcessing: RecordProcessingConfig<K, V> = RecordProcessingConfig()
    private var offsetHandling: OffsetHandlingConfig = OffsetHandlingConfig()

    fun bootstrapServers(servers: String) {
        bootstrapServers = servers
    }

    fun consumerGroup(consumerGroup: String) {
        this.consumerGroup = consumerGroup
    }

    fun topics(vararg topics: String) {
        this.topics = topics.toList()
    }

    fun keyDeserializer(deserializer: KClass<out Deserializer<*>>) {
        keyDeserializer = deserializer
    }

    fun valueDeserializer(deserializer: KClass<out Deserializer<*>>) {
        valueDeserializer = deserializer
    }

    fun polling(block: PollingConfigBuilder.() -> Unit) {
        polling = PollingConfigBuilder().apply(block).build()
    }

    fun recordProcessing(block: RecordProcessingConfigBuilder<K, V>.() -> Unit) {
        recordProcessing = RecordProcessingConfigBuilder<K, V>().apply(block).build()
    }

    fun offsetHandling(block: OffsetHandlingConfigBuilder.() -> Unit) {
        offsetHandling = OffsetHandlingConfigBuilder().apply(block).build()
    }

    fun build(): ConsumerConfig<K, V> {
        require(topics.isNotEmpty()) { "At least one topic must be specified" }

        return ConsumerConfig(
            bootstrapServers = bootstrapServers,
            consumerGroup = consumerGroup,
            topics = topics,
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            polling = polling,
            recordProcessing = recordProcessing,
            offsetHandling = offsetHandling
        )
    }
}

class PollingConfigBuilder {
    private var maxPollRecords: Int = 500
    private var pollTimeout: Duration = 100.milliseconds
    private var pollInterval: Duration = 500.milliseconds
    private var maxPollInterval: Duration = 5.minutes

    fun maxPollRecords(records: Int) {
        maxPollRecords = records
    }

    fun pollTimeout(timeout: Duration) {
        pollTimeout = timeout
    }

    fun pollInterval(interval: Duration) {
        pollInterval = interval
    }

    fun maxPollInterval(interval: Duration) {
        maxPollInterval = interval
    }

    fun build(): PollingConfig =
        PollingConfig(
            maxPollRecords = maxPollRecords,
            pollTimeout = pollTimeout,
            pollInterval = pollInterval,
            maxPollInterval = maxPollInterval
        )
}

class RecordProcessingConfigBuilder<K, V> {
    private var queueSize: Int = 10
    private var concurrency: Int = 5
    private var recordProcessor: RecordProcessor<K, V> = {}

    fun queueSize(queueSize: Int) {
        this.queueSize = queueSize
    }

    fun concurrency(concurrency: Int) {
        this.concurrency = concurrency
    }

    fun processor(processor: RecordProcessor<K, V>) {
        recordProcessor = processor
    }

    fun build(): RecordProcessingConfig<K, V> =
        RecordProcessingConfig(
            queueSize = queueSize,
            concurrency = concurrency,
            processor = recordProcessor
        )
}

class OffsetHandlingConfigBuilder {
    private var offsetCommitInterval: Duration = 30.seconds
    private var autoOffsetReset = OffsetHandlingConfig.AutoOffsetReset.LATEST

    fun offsetCommitInterval(interval: Duration) {
        offsetCommitInterval = interval
    }

    fun autoOffsetReset(offsetReset: OffsetHandlingConfig.AutoOffsetReset) {
        autoOffsetReset = offsetReset
    }

    fun build(): OffsetHandlingConfig =
        OffsetHandlingConfig(
            offsetCommitInterval = offsetCommitInterval,
            autoOffsetReset = autoOffsetReset
        )
}
