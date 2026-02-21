package coroutines.mastery.kafka.consumer.config

import coroutines.mastery.kafka.consumer.customers.CustomerRecordProcessor
import coroutines.mastery.kafka.consumer.library.AutoOffsetReset
import coroutines.mastery.kafka.consumer.library.Consumers
import coroutines.mastery.kafka.consumer.library.KafkaProperties
import kotlinx.coroutines.CoroutineScope
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import kotlin.time.Duration.Companion.minutes

@Configuration
class KafkaConsumerConfig {

    @Bean
    fun customerConsumer(
        recordProcessor: CustomerRecordProcessor,
        backgroundScope: CoroutineScope,
        @Value($$"${spring.kafka.bootstrap-servers}") servers: String
    ) =
        Consumers.start(
            kafkaProperties = KafkaProperties(
                bootstrapServers = servers,
                consumerGroup = "customers-consumer-group",
                keyDeserializer = StringDeserializer::class,
                valueDeserializer = StringDeserializer::class,
                maxPollRecords = 100,
                maxPollInterval = 1.minutes,
                autoOffsetReset = AutoOffsetReset.EARLIEST
            ),
            topics = listOf("customers"),
            recordProcessor = recordProcessor,
            backgroundScope = backgroundScope
        )
}
