package coroutines.mastery.kafka.consumer.config

import coroutines.mastery.kafka.consumer.customers.CustomerRecordProcessor
import coroutines.mastery.kafka.consumer.library.builder.kafkaConsumer
import coroutines.mastery.kafka.consumer.library.config.OffsetHandlingConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes

@Configuration
class KafkaConsumerConfig {

    @Bean
    fun customerConsumer(
        recordProcessor: CustomerRecordProcessor,
        @Value($$"${spring.kafka.bootstrap-servers}") servers: String
    ) =
        kafkaConsumer {
            bootstrapServers(servers)
            consumerGroup("customers-consumer-group")
            topics("customers")
            keyDeserializer(StringDeserializer::class)
            valueDeserializer(StringDeserializer::class)
            polling {
                maxPollRecords(100)
                pollTimeout(100.milliseconds)
                pollInterval(500.milliseconds)
                maxPollInterval(1.minutes)
            }
            recordProcessing {
                queueSize(10)
                concurrency(5)
                processor(recordProcessor)
            }
            offsetHandling {
                offsetCommitInterval(1.minutes)
                autoOffsetReset(OffsetHandlingConfig.AutoOffsetReset.EARLIEST)
            }
        }
}
