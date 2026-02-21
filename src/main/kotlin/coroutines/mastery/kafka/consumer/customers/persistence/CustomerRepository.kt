package coroutines.mastery.kafka.consumer.customers.persistence

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.util.*

@Repository
interface CustomerRepository : JpaRepository<CustomerEntity, Long> {

    fun findByCustomerId(customerId: UUID): CustomerEntity?

    @Modifying
    @Transactional
    @Query("DELETE FROM CustomerEntity c WHERE c.customerId = :customerId")
    fun deleteByCustomerId(customerId: UUID): Int
}
