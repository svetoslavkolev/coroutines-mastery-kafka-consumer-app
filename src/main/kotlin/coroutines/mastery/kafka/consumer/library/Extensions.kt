package coroutines.mastery.kafka.consumer.library

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

suspend fun <T> Iterable<T>.forEachAsync(
    concurrency: Int = 16,
    action: suspend (T) -> Unit
) {
    val semaphore = Semaphore(concurrency)
    coroutineScope {
        forEach {
            launch {
                semaphore.withPermit { action(it) }
            }
        }
    }
}
