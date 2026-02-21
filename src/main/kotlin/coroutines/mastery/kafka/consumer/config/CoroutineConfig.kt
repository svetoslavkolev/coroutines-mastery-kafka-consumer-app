package coroutines.mastery.kafka.consumer.config

import kotlinx.coroutines.*
import mu.KotlinLogging
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Configuration
class CoroutineConfig {

    private val log = KotlinLogging.logger {}

    @Bean
    fun ioExecutor(): ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    @Bean
    fun ioDispatcher(ioExecutor: ExecutorService): CoroutineDispatcher =
        ioExecutor.asCoroutineDispatcher()

    @Bean
    fun backgroundScope() =
        CoroutineScope(SupervisorJob() + CoroutineExceptionHandler { _, throwable ->
            log.error(throwable) { "Exception occurred in a coroutine: ${throwable.message}" }
        })
}
