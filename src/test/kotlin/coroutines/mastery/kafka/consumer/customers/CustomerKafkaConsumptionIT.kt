package coroutines.mastery.kafka.consumer.customers

import coroutines.mastery.kafka.consumer.customers.persistence.CustomerRepository
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.untilNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import tools.jackson.databind.json.JsonMapper
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.test.*

@SpringBootTest
@ActiveProfiles("it-test")
class CustomerKafkaConsumptionIT {

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Autowired
    private lateinit var jsonMapper: JsonMapper

    @Autowired
    private lateinit var customerRepository: CustomerRepository

    private lateinit var customer: Customer
    private val topic = "customers"

    companion object {
        private val kafka = KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"))

        init {
            kafka.start()
        }

        @JvmStatic
        @DynamicPropertySource
        fun kafkaProperties(registry: DynamicPropertyRegistry) {
            registry.add("spring.kafka.bootstrap-servers") { kafka.bootstrapServers }
        }
    }

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
    fun `should create a new customer from Kafka message`() {
        runBlocking {
            // When - Send customer message to Kafka
            val customerJson = jsonMapper.writeValueAsString(customer)
            kafkaTemplate.send(topic, customer.customerId.toString(), customerJson).get()

            // Then - Customer should be in database
            val customerEntity = await.atMost(5, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilNotNull {
                    customerRepository.findByCustomerId(customer.customerId)
                }

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
    fun `should update an existing customer from Kafka message`() {
        runBlocking {
            // Given - Create initial customer via Kafka
            val customerJson = jsonMapper.writeValueAsString(customer)
            kafkaTemplate.send(topic, customer.customerId.toString(), customerJson).get()

            // Wait for customer to be created
            await.atMost(5, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilNotNull {
                    customerRepository.findByCustomerId(customer.customerId)
                }

            // When - Send updated customer message to Kafka
            val updatedCustomer = customer.copy(
                email = "ivan.petrov2@example2.com",
                phoneNumber = "123-456-7890"
            )
            val updatedCustomerJson = jsonMapper.writeValueAsString(updatedCustomer)
            kafkaTemplate.send(topic, updatedCustomer.customerId.toString(), updatedCustomerJson)
                .get()

            // Wait for customer to be updated
            await.atMost(5, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until {
                    val entity = customerRepository.findByCustomerId(customer.customerId)
                    entity?.email == updatedCustomer.email
                }

            // Then - Customer should be updated in database
            val updatedEntity = customerRepository.findByCustomerId(customer.customerId)
            assertNotNull(updatedEntity)

            assertEquals(updatedCustomer.email, updatedEntity.email)
            assertEquals(updatedCustomer.phoneNumber, updatedEntity.phoneNumber)
        }
    }

    @Test
    fun `should delete an existing customer from Kafka tombstone message`() {
        runBlocking {
            // Given - Create customer via Kafka
            val customerJson = jsonMapper.writeValueAsString(customer)
            kafkaTemplate.send(topic, customer.customerId.toString(), customerJson).get()

            // Wait for customer to be created
            await.atMost(5, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilNotNull {
                    customerRepository.findByCustomerId(customer.customerId)
                }

            // When - Send tombstone message (null value) to Kafka
            kafkaTemplate.send(topic, customer.customerId.toString(), null).get()

            // Wait for customer to be deleted
            await.atMost(5, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilNull {
                    customerRepository.findByCustomerId(customer.customerId)
                }

            // Then - Customer should be deleted from database
            assertNull(customerRepository.findByCustomerId(customer.customerId))
        }
    }
}
