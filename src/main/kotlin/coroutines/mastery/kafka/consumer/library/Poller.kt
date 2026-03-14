package coroutines.mastery.kafka.consumer.library

import coroutines.mastery.kafka.consumer.library.config.PollingConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.isActive
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecords

interface RecordsFlow<K, V> {
    fun observeRecords(): Flow<ConsumerRecords<K, V>>
}

class Poller<K, V>(
    consumer: Consumer<K, V>,
    pollingConfig: PollingConfig,
    backgroundScope: CoroutineScope
) : RecordsFlow<K, V> {

    private val log = KotlinLogging.logger {}

    private val recordsFlow: SharedFlow<ConsumerRecords<K, V>> = flow {
        while (currentCoroutineContext().isActive) {
            log.info { "Polling..." }
            val records = consumer.poll(pollingConfig.pollTimeout)

            if (!records.isEmpty) {
                val recordCount = records.count()
                log.info { "Emitting $recordCount records..." }
                emit(records)
                log.info { "Emitted $recordCount records." }
            }
            delay(pollingConfig.pollInterval)
        }
    }
        .onStart {
            log.info {
                "Starting polling for Kafka records with pollTimeout = ${pollingConfig.pollTimeout} " +
                        "and pollInterval = ${pollingConfig.pollInterval}..."
            }
        }
        .onCompletion { log.info { "Stopped polling for Kafka records." } }
        .shareIn( // ensure polling happens only once in case of multiple subscribers
            scope = backgroundScope,
            started = SharingStarted.Lazily
        )

    override fun observeRecords(): Flow<ConsumerRecords<K, V>> = recordsFlow
}
