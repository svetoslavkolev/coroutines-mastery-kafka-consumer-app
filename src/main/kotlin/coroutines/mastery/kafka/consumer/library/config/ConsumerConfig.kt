package coroutines.mastery.kafka.consumer.library.config

import coroutines.mastery.kafka.consumer.library.RecordProcessor
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.*
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

data class ConsumerConfig<K, V>(
    val bootstrapServers: String,
    val consumerGroup: String,
    val topics: List<String> = emptyList(),
    val keyDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class,
    val valueDeserializer: KClass<out Deserializer<*>> = StringDeserializer::class,
    val polling: PollingConfig = PollingConfig(),
    val recordProcessing: RecordProcessingConfig<K, V> = RecordProcessingConfig(),
    val offsetHandling: OffsetHandlingConfig = OffsetHandlingConfig(),
)

data class PollingConfig(
    val maxPollRecords: Int = 500,
    val pollTimeout: Duration = 100.milliseconds,
    val pollInterval: Duration = 500.milliseconds,
    val maxPollInterval: Duration = 5.minutes,
)

data class RecordProcessingConfig<K, V>(
    val queueSize: Int = 10,
    val concurrency: Int = 5,
    val processor: RecordProcessor<K, V> = {},
)

data class OffsetHandlingConfig(
    val offsetCommitInterval: Duration = 30.seconds,
    val autoOffsetReset: AutoOffsetReset = AutoOffsetReset.LATEST,
) {
    enum class AutoOffsetReset {
        EARLIEST, LATEST, NONE
    }
}

fun <K, V> ConsumerConfig<K, V>.toKafkaProperties(): Properties {
    val props = Properties()
    props["bootstrap.servers"] = bootstrapServers
    props["group.id"] = consumerGroup
    props["key.deserializer"] = keyDeserializer.qualifiedName
    props["value.deserializer"] = valueDeserializer.qualifiedName
    props["max.poll.records"] = polling.maxPollRecords.toString()
    props["max.poll.interval.ms"] = polling.maxPollInterval.inWholeMilliseconds.toString()
    props["enable.auto.commit"] = "false"
    props["auto.offset.reset"] = offsetHandling.autoOffsetReset.name.lowercase()
    return props
}
