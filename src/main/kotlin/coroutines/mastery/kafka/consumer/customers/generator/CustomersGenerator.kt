package coroutines.mastery.kafka.consumer.customers.generator

import coroutines.mastery.kafka.consumer.customers.Customer
import mu.KotlinLogging
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.util.*
import java.util.concurrent.TimeUnit

@Component
class CustomersGenerator(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) {

    private val log = KotlinLogging.logger {}

    @Scheduled(fixedDelay = 3, timeUnit = TimeUnit.MINUTES)
    fun generateCustomers() {
        log.info { "Generating customers" }
        // Generate and send 10,000 customers
        repeat(10) {
            val firstName = generateRandomName(8)
            val lastName = generateRandomName(12)

            val customer = Customer(
                customerId = UUID.randomUUID(),
                firstName = firstName,
                lastName = lastName,
                email = "${firstName.lowercase()}.${lastName.lowercase()}@example.com",
                address =
                    "${(100..9999).random()} ${generateRandomName(10)} St, City, ST ${(10000..99999).random()}",
                phoneNumber = "+1${(1000000000..9999999999).random()}"
            )

            kafkaTemplate.send(
                "customers",
                customer.customerId.toString(),
                objectMapper.writeValueAsString(customer)
            )
        }

        log.info { "Finished generating customers" }
    }

    private fun generateRandomName(maxLength: Int): String {
        val length = (3..maxLength).random()
        return (1..length)
            .map { ('a'..'z').random() }
            .joinToString("").replaceFirstChar { it.uppercase() }
    }
}
