package coroutines.mastery.kafka.consumer.customers

import coroutines.mastery.kafka.consumer.library.RecordProcessor
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper

@Component
class CustomerRecordProcessor(
    private val customerService: CustomerService,
    private val objectMapper: ObjectMapper
) : RecordProcessor<String, String> {

    private val log = KotlinLogging.logger {}

    override suspend fun process(record: ConsumerRecord<String, String>) {
        log.info {
            "Processing kafka record from topic ${record.topic()}, partition ${record.partition()} " +
                    "and offset ${record.offset()}..."
        }
        val customer = objectMapper.readValue(record.value(), Customer::class.java)
        customerService.upsertCustomer(customer)
    }
}
