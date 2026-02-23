package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
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
            .onStart { log.info { "Start collecting latest offsets..." } }
            .onCompletion { log.info { "Stopped collecting latest offsets." } }
            .launchIn(backgroundScope)

        backgroundScope.launch {
            try {
                while (true) {
                    delay(500000)
                    mutex.withLock {
                        log.info { "Committing offsets $nextOffsets" }
                        consumer.commitOffsets(nextOffsets)
                        nextOffsets.clear()
                    }
                }
            } finally {
                withContext(NonCancellable) {
                    mutex.withLock {
                        log.info { "Committing offsets before quitting: $nextOffsets" }
                        consumer.commitOffsetsSync(nextOffsets)
                        nextOffsets.clear()
                    }
                }
            }
        }
    }
}
