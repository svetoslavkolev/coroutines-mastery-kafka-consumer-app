package coroutines.mastery.kafka.consumer.customers

import coroutines.mastery.kafka.consumer.customers.persistence.CustomerRepository
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import org.springframework.stereotype.Service
import java.util.*

@Service
class CustomerService(
    private val customerRepository: CustomerRepository,
    private val coroutineDispatcher: CoroutineDispatcher
) {

    private val log = KotlinLogging.logger {}

    suspend fun upsertCustomer(customer: Customer) {
        withContext(coroutineDispatcher) {
            log.info { "Upserting customer $customer" }
            val customerEntity = customerRepository.findByCustomerId(customer.customerId)
            customerRepository.save(customerEntity?.updateFrom(customer) ?: customer.toEntity())
        }
    }

    suspend fun deleteCustomer(customerId: UUID) {
        withContext(coroutineDispatcher) {
            log.info { "Deleting customer $customerId" }

            when (customerRepository.deleteByCustomerId(customerId)) {
                0 -> log.warn { "Customer with customerId $customerId not found for deletion" }
                else -> log.info { "Deleted customer with customerId: $customerId" }
            }
        }
    }
}
