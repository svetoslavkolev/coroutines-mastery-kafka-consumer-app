package coroutines.mastery.kafka.consumer.customers

import coroutines.mastery.kafka.consumer.customers.persistence.CustomerEntity
import java.util.*

data class Customer(
    val customerId: UUID,
    val firstName: String,
    val lastName: String,
    val email: String,
    val address: String,
    val phoneNumber: String?,
)

fun Customer.toEntity() = CustomerEntity(
    customerId = this.customerId,
    firstName = this.firstName,
    lastName = this.lastName,
    email = this.email,
    address = this.address,
    phoneNumber = this.phoneNumber,
)

fun CustomerEntity.updateFrom(customer: Customer): CustomerEntity {
    this.firstName = customer.firstName
    this.lastName = customer.lastName
    this.email = customer.email
    this.address = customer.address
    this.phoneNumber = customer.phoneNumber
    return this
}

