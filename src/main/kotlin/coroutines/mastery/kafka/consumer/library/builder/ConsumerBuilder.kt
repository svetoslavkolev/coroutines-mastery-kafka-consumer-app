package coroutines.mastery.kafka.consumer.library.builder

import coroutines.mastery.kafka.consumer.library.*
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

fun <K, V> kafkaConsumer(block: ConsumerConfigBuilder<K, V>.() -> Unit): ConsumerContext<K, V> {
    return ConsumerConfigBuilder<K, V>().apply(block).build()
}

class ConsumerConfigBuilder<K, V> {
    private lateinit var kafkaProperties: KafkaProperties
    private var topics: List<String> = emptyList()
    private lateinit var recordProcessor: RecordProcessor<K, V>

    fun config(block: KafkaPropertiesBuilder.() -> Unit) {
        kafkaProperties = KafkaPropertiesBuilder().apply(block).build()
    }

    fun topics(vararg topics: String) {
        this.topics = topics.toList()
    }

    fun processor(processor: RecordProcessor<K, V>) {
        recordProcessor = processor
    }

    fun build(): ConsumerContext<K, V> {
        require(topics.isNotEmpty()) { "At least one topic must be specified" }

        return Consumers.start(
            kafkaProperties = kafkaProperties,
            topics = topics,
            recordProcessor = recordProcessor,
        )
    }
}

class KafkaPropertiesBuilder {
    private lateinit var bootstrapServers: String
    private lateinit var consumerGroup: String
    private var keyDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class
    private var valueDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class
    private var maxPollRecords: Int = 500
    private var maxPollInterval: Duration = 5.minutes
    private var autoOffsetReset: AutoOffsetReset = AutoOffsetReset.LATEST

    fun bootstrapServers(servers: String) {
        bootstrapServers = servers
    }

    fun consumerGroup(consumerGroup: String) {
        this.consumerGroup = consumerGroup
    }

    fun keyDeserializer(deserializer: KClass<out Deserializer<*>>) {
        keyDeserializer = deserializer
    }

    fun valueDeserializer(deserializer: KClass<out Deserializer<*>>) {
        valueDeserializer = deserializer
    }

    fun maxPollRecords(records: Int) {
        maxPollRecords = records
    }

    fun maxPollInterval(interval: Duration) {
        maxPollInterval = interval
    }

    fun autoOffsetReset(offsetReset: AutoOffsetReset) {
        autoOffsetReset = offsetReset
    }

    fun build(): KafkaProperties =
        KafkaProperties(
            bootstrapServers = bootstrapServers,
            consumerGroup = consumerGroup,
            keyDeserializer = keyDeserializer,
            valueDeserializer = valueDeserializer,
            maxPollRecords = maxPollRecords,
            maxPollInterval = maxPollInterval,
            autoOffsetReset = autoOffsetReset
        )
}
