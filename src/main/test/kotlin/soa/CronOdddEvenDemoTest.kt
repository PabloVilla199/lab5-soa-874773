package soa

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.ApplicationContext
import org.springframework.integration.channel.QueueChannel
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.PublishSubscribeChannelSpec
import org.springframework.messaging.Message
import org.awaitility.Awaitility.await
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

@SpringBootTest
class CronOddEvenDemoTest {

    @Autowired
    lateinit var sendNumber: SendNumber

    @Autowired
    lateinit var integerSource: AtomicInteger

    @Autowired
    lateinit var context: ApplicationContext

    private fun subscribeToChannel(beanName: String): QueueChannel {
        val original = context.getBean(beanName)
        val queue = QueueChannel()
        when (original) {
            is PublishSubscribeChannelSpec<*> -> {
                val channel = context.getBean(beanName) as org.springframework.integration.channel.PublishSubscribeChannel
                channel.subscribe { queue.send(it) }
            }
            else -> {
                val channel = context.getBean(beanName) as org.springframework.integration.channel.DirectChannel
                channel.subscribe { queue.send(it) }
            }
        }
        return queue
    }

    @Test
    fun `application context loads`() {
        assertNotNull(context)
    }

    @Test
    fun `atomic integer increments`() {
        val before = integerSource.get()
        integerSource.incrementAndGet()
        assert(integerSource.get() == before + 1)
    }

    @Test
    fun `even numbers go to evenFlow`() {
        val evenQueue = subscribeToChannel("evenChannel")
        sendNumber.sendNumber(4)
        val msg: Message<*> = await().atMost(1, TimeUnit.SECONDS).until({ evenQueue.receive() }, { it != null })!!
        assertEquals("Number 4", msg.payload)
    }

    @Test
    fun `odd numbers go to oddFlow`() {
        val oddQueue = subscribeToChannel("oddChannel")
        sendNumber.sendNumber(5)
        val msg: Message<*> = await().atMost(1, TimeUnit.SECONDS).until({ oddQueue.receive() }, { it != null })!!
        assertEquals("Number 5", msg.payload)
    }

    @Test
    fun `negative numbers are processed`() {
        val oddQueue = subscribeToChannel("oddChannel")
        sendNumber.sendNumber(-7)
        val msg: Message<*> = await().atMost(1, TimeUnit.SECONDS).until({ oddQueue.receive() }, { it != null })!!
        assertEquals("Number -7", msg.payload)
    }
}
