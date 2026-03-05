package coroutines.mastery.kafka.consumer.config

import coroutines.mastery.kafka.consumer.customers.CustomerRecordProcessor
import coroutines.mastery.kafka.consumer.library.AutoOffsetReset
import coroutines.mastery.kafka.consumer.library.builder.kafkaConsumer
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
        @Value($$"${spring.kafka.bootstrap-servers}") servers: String
    ) =
        kafkaConsumer {
            config {
                bootstrapServers(servers)
                consumerGroup("customers-consumer-group")
                keyDeserializer(StringDeserializer::class)
                valueDeserializer(StringDeserializer::class)
                maxPollRecords(100)
                maxPollInterval(1.minutes)
                autoOffsetReset(AutoOffsetReset.EARLIEST)
            }
            topics("customers")
            processor(recordProcessor)
        }
}
