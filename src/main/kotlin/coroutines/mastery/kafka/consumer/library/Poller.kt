package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecords
import kotlin.time.Duration.Companion.milliseconds

class Poller<K, V>(
    private val consumer: Consumer<K, V>,
    backgroundScope: CoroutineScope
) {

    private val log = KotlinLogging.logger {}

    private val recordsFlow: SharedFlow<ConsumerRecords<K, V>> = flow {
        while (true) {
            log.info { "Polling..." }
            val records = consumer.poll(100.milliseconds)

            if (!records.isEmpty) {
                val recordCount = records.count()
                log.info { "Emitting $recordCount records..." }
                emit(records)
                log.info { "Emitted $recordCount records." }
            }
            delay(500)
        }
    }
        .onStart { log.info { "Starting polling for Kafka records..." } }
        .onCompletion { log.info { "Stopped polling for Kafka records." } }
        .shareIn( // ensure polling happens only once in case of multiple subscribers
            scope = backgroundScope,
            started = SharingStarted.WhileSubscribed()
        )

    fun observeRecords(): Flow<ConsumerRecords<K, V>> = recordsFlow
}
