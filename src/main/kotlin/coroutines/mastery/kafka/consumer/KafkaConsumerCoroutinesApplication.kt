package coroutines.mastery.kafka.consumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.persistence.autoconfigure.EntityScan
import org.springframework.boot.runApplication
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableJpaRepositories
@EntityScan
@EnableScheduling
class KafkaConsumerCoroutinesApplication

fun main(args: Array<String>) {
    runApplication<KafkaConsumerCoroutinesApplication>(*args)
}
