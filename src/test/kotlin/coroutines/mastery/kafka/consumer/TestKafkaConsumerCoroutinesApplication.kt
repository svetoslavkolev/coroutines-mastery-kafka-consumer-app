package coroutines.mastery.kafka.consumer

import org.springframework.boot.fromApplication
import org.springframework.boot.with


fun main(args: Array<String>) {
    fromApplication<KafkaConsumerCoroutinesApplication>().with(TestcontainersConfiguration::class)
        .run(*args)
}
