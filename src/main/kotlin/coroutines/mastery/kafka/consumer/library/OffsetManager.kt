package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class OffsetManager<K, V>(
    consumer: Consumer<K, V>,
    executor: Executor<K, V>,
    backgroundScope: CoroutineScope,
) {

    private val log = KotlinLogging.logger {}
    private val nextOffsets = ConcurrentHashMap<TopicPartition, OffsetAndMetadata>()
    private val mutex = Mutex()

    init {
        executor.observeLatestOffsets()
            .onEach { event ->
                mutex.withLock {
                    nextOffsets[event.partition] = OffsetAndMetadata(
                        event.latestOffset + 1, // offset of the next record should be committed
                        Optional.ofNullable(event.leaderEpoch),
                        ""
                    )
                }
            }
            .launchIn(backgroundScope)

        backgroundScope.launch {
            while (true) {
                delay(5000)
                mutex.withLock {
                    log.info { "Committing offsets $nextOffsets" }
                    consumer.commitOffsets(nextOffsets)
                    nextOffsets.clear()
                }
            }
        }
    }
}
