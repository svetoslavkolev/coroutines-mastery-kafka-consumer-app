package coroutines.mastery.kafka.consumer.config

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Configuration
class CoroutineConfig {

    @Bean
    fun ioExecutor(): ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    @Bean
    fun ioDispatcher(ioExecutor: ExecutorService): CoroutineDispatcher =
        ioExecutor.asCoroutineDispatcher()

    @Bean
    fun backgroundScope() = CoroutineScope(SupervisorJob())
}
