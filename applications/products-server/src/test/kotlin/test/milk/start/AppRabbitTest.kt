package test.milk.start

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import io.ktor.http.*
import io.ktor.server.testing.*
import io.milk.products.PurchaseInfo
import io.milk.rabbitmq.BasicRabbitConfiguration
import io.milk.rabbitmq.RabbitTestSupport
import io.milk.start.module
import io.milk.testsupport.testDbPassword
import io.milk.testsupport.testDbUsername
import io.milk.testsupport.testJdbcUrl
import io.mockk.clearAllMocks
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import test.milk.TestScenarioSupport
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

// TODO DONE - MESSAGING - Remove the @Ignore annotation - Done

class AppRabbitTest {
    private val testSupport = RabbitTestSupport()
    private val engine = TestApplicationEngine()
    private val mapper: ObjectMapper = ObjectMapper().registerKotlinModule()

    @Before
    fun before() {
        BasicRabbitConfiguration("products-exchange", "products", "auto").setUp()
        testSupport.purge("products")

        BasicRabbitConfiguration("products-exchange", "safer-products", "safer").setUp()
        testSupport.purge("safer-products")

        clearAllMocks()
        TestScenarioSupport().loadTestScenario("products")
        engine.start(wait = false)
        engine.application.module(testJdbcUrl, testDbUsername, testDbPassword)
    }

    @Test
    fun testQuantity_1() {
        makePurchase(PurchaseInfo(105442, "milk", 1), routingKey = "auto")
        testSupport.waitForConsumers("products")

        with(engine) {
            with(handleRequest(HttpMethod.Get, "/")) {
                val compact = response.content!!.replace("\\s".toRegex(), "")
                val milk = "<td>milk</td><td>([0-9]+)</td>".toRegex().find(compact)!!.groups[1]!!.value
                assertEquals(130, milk.toInt())
            }
        }
    }

    @Test
    fun testQuantity_50() {
        makePurchases(PurchaseInfo(105442, "milk", 1), routingKey = "auto")
        testSupport.waitForConsumers("products")

        runBlocking {
            delay(5000) // delay for 1 second
        }

        with(engine) {
            with(handleRequest(HttpMethod.Get, "/")) {
                val compact = response.content!!.replace("\\s".toRegex(), "")
                val milk = "<td>milk</td><td>([0-9]+)</td>".toRegex().find(compact)!!.groups[1]!!.value
                assertEquals(81, milk.toInt())
            }
        }
    }

    @Test
    fun testSaferQuantity() {
        // TODO DONE - MESSAGING -
        //  test a "safer" purchase, one where you are using a different "safer" queue
        //  then wait for consumers,
        //  then make a request
        //  and assert that the milk count 130

        // Make a "safer" purchase
        makePurchase(PurchaseInfo(105442, "milk", 1), routingKey = "safer")
        // Wait for consumers
        testSupport.waitForConsumers("safer-products")

        // Make a GET request
        with(engine) {
            with(handleRequest(HttpMethod.Get, "/")) {
                val compact = response.content!!.replace("\\s".toRegex(), "")
                val milk = "<td>milk</td><td>([0-9]+)</td>".toRegex().find(compact)!!.groups[1]!!.value
                // Assert that the milk count is 130
                assertEquals(130, milk.toInt())
            }
        }

    }

    @Test
    fun testBestCase() {
        makePurchases(PurchaseInfo(105443, "bacon", 1), routingKey = "safer")
        // TODO DONE - MESSAGING -
        // Uncomment the below after introducing the safer product update handler with manual acknowledgement
        testSupport.waitForConsumers("safer-products")

        with(engine) {
            with(handleRequest(HttpMethod.Get, "/")) {
                val compact = response.content!!.replace("\\s".toRegex(), "")
                val bacon = "<td>bacon</td><td>([0-9]+)</td>".toRegex().find(compact)!!.groups[1]!!.value
                assertTrue(bacon.toInt() < 72, "expected ${bacon.toInt()} to be less than 72")
            }
        }
    }



    private fun makePurchase(purchase: PurchaseInfo, routingKey: String) {
        val factory = ConnectionFactory().apply { useNio() }
        factory.newConnection().use { connection ->
            connection.createChannel().use { channel ->
                val body = mapper.writeValueAsString(purchase).toByteArray()
                channel.basicPublish("products-exchange", routingKey, MessageProperties.BASIC, body)
            }
        }
    }

    private fun makePurchases(purchase: PurchaseInfo, routingKey: String) {
        val factory = ConnectionFactory().apply { useNio() }
        factory.newConnection().use { connection ->
            connection.createChannel().use { channel ->
                (1..50).map {
                    val body = mapper.writeValueAsString(purchase).toByteArray()
                    channel.basicPublish("products-exchange", routingKey, MessageProperties.PERSISTENT_BASIC, body)
                }
            }
        }
    }
}
