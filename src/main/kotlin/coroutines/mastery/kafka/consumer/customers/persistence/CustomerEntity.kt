package coroutines.mastery.kafka.consumer.customers.persistence

import jakarta.persistence.*
import org.hibernate.annotations.CreationTimestamp
import org.hibernate.annotations.UpdateTimestamp
import java.time.Instant
import java.util.*

@Entity
@Table(name = "customers")
class CustomerEntity(

    @Column(name = "customer_id", nullable = false)
    var customerId: UUID,

    @Column(name = "first_name", nullable = false, length = 64)
    var firstName: String,

    @Column(name = "last_name", nullable = false, length = 64)
    var lastName: String,

    @Column(name = "email", nullable = false)
    var email: String,

    @Column(name = "address", nullable = false, length = 500)
    var address: String,

    @Column(name = "phone_number", length = 20)
    var phoneNumber: String? = null,
) {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null

    @CreationTimestamp
    @Column(name = "created_at", nullable = false)
    lateinit var createdAt: Instant

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    lateinit var updatedAt: Instant
}
