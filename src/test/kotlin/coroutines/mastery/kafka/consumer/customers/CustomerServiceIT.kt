package coroutines.mastery.kafka.consumer.customers

import coroutines.mastery.kafka.consumer.TestcontainersConfiguration
import coroutines.mastery.kafka.consumer.customers.persistence.CustomerRepository
import kotlinx.coroutines.runBlocking
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import java.util.*
import kotlin.test.*

@Import(TestcontainersConfiguration::class)
@SpringBootTest
class CustomerServiceIT {

    @Autowired
    private lateinit var customerService: CustomerService

    @Autowired
    private lateinit var customerRepository: CustomerRepository

    private lateinit var customer: Customer

    @BeforeTest
    fun setup() {
        customer = Customer(
            customerId = UUID.randomUUID(),
            firstName = "Ivan",
            lastName = "Petrov",
            email = "ivan.petrov@example.com",
            address = "456 First St, Sofia, Bulgaria",
            phoneNumber = "987-654-3210"
        )
    }

    @AfterTest
    fun tearDown() {
        customerRepository.deleteAll()
    }

    @Test
    fun `should create a new customer`() {
        runBlocking { // no need for virtual time
            customerService.upsertCustomer(customer)

            val customerEntity = customerRepository.findByCustomerId(customer.customerId)
            assertNotNull(customerEntity)

            assertNotNull(customerEntity.id)
            assertNotNull(customerEntity.createdAt)
            assertNotNull(customerEntity.updatedAt)

            assertEquals(customer.customerId, customerEntity.customerId)
            assertEquals(customer.firstName, customerEntity.firstName)
            assertEquals(customer.lastName, customerEntity.lastName)
            assertEquals(customer.email, customerEntity.email)
            assertEquals(customer.address, customerEntity.address)
            assertEquals(customer.phoneNumber, customerEntity.phoneNumber)
        }
    }

    @Test
    fun `should update an existing customer`() {
        runBlocking {
            // given
            customerService.upsertCustomer(customer)

            var customerEntity = customerRepository.findByCustomerId(customer.customerId)
            assertNotNull(customerEntity)

            // when
            val updatedCustomer = customer.copy(
                email = "ivan.petrov2@example2.com",
                phoneNumber = "123-456-7890"
            )

            customerService.upsertCustomer(updatedCustomer)

            // then
            customerEntity = customerRepository.findByCustomerId(customer.customerId)
            assertNotNull(customerEntity)

            assertEquals(updatedCustomer.email, customerEntity.email)
            assertEquals(updatedCustomer.phoneNumber, customerEntity.phoneNumber)
        }
    }

    @Test
    fun `should delete an existing customer`() {
        runBlocking {
            // given
            customerService.upsertCustomer(customer)

            var customerEntity = customerRepository.findByCustomerId(customer.customerId)
            assertNotNull(customerEntity)

            // when
            customerService.deleteCustomer(customer.customerId)

            // then
            customerEntity = customerRepository.findByCustomerId(customer.customerId)
            assertNull(customerEntity)
        }
    }
}
