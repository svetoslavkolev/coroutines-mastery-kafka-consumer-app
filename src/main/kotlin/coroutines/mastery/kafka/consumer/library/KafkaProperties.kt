package coroutines.mastery.kafka.consumer.library

import org.apache.kafka.common.serialization.Deserializer
import java.util.*
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

enum class AutoOffsetReset {
    EARLIEST, LATEST, NONE
}

data class KafkaProperties(
    val bootstrapServers: String,
    val consumerGroup: String,
    val keyDeserializer: KClass<out Deserializer<*>>,
    val valueDeserializer: KClass<out Deserializer<*>>,
    val maxPollRecords: Int = 500,
    val maxPollInterval: Duration = 5.minutes,
    val autoOffsetReset: AutoOffsetReset = AutoOffsetReset.LATEST,
)

fun KafkaProperties.toProps(): Properties {
    val props = Properties()
    props["bootstrap.servers"] = bootstrapServers
    props["group.id"] = consumerGroup
    props["key.deserializer"] = keyDeserializer.qualifiedName
    props["value.deserializer"] = valueDeserializer.qualifiedName
    props["max.poll.records"] = maxPollRecords.toString()
    props["max.poll.interval.ms"] = maxPollInterval.inWholeMilliseconds.toString()
    props["enable.auto.commit"] = "false"
    props["auto.offset.reset"] = autoOffsetReset.name.lowercase()
    return props
}
