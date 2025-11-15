@file:Suppress("WildcardImport", "NoWildcardImports", "MagicNumber")

package soa

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.integration.annotation.Gateway
import org.springframework.integration.annotation.MessagingGateway
import org.springframework.integration.config.EnableIntegration
import org.springframework.integration.dsl.IntegrationFlow
import org.springframework.integration.dsl.MessageChannels
import org.springframework.integration.dsl.Pollers
import org.springframework.integration.dsl.PublishSubscribeChannelSpec
import org.springframework.integration.dsl.integrationFlow
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

private val logger = LoggerFactory.getLogger("soa.CronOddEvenDemo")

/**
 * Spring Boot + Spring Integration application demonstrating Enterprise Integration Patterns.
 *
 * Esta versión mantiene:
 * - Canal central 'numberChannel' para todos los números.
 * - Flujo de números impares serial, con filtro.
 * - Servicio Odd ya no se usa como @ServiceActivator, el flujo se cierra internamente.
 *
 * Cambios respecto a la v1 original:
 * - oddChannel convertido a Publish-Subscribe.
 *
 * El flujo general:
 * 1. Generación secuencial de números (AtomicInteger).
 * 2. Envío al canal central 'numberChannel'.
 * 3. Router determina si el número es par o impar.
 * 4. Even numbers → evenFlow (transformación y log).
 * 5. Odd numbers → oddFlow (filtro, transformación, handler final).
 */
@SpringBootApplication
@EnableIntegration
@EnableScheduling
class IntegrationApplication(
    private val sendNumber: SendNumber,
) {

    /**
     * Fuente de números secuenciales.
     * Bean de tipo AtomicInteger.
     */
    @Bean
    fun integerSource(): AtomicInteger = AtomicInteger()

    /**
     * Canal central donde convergen todos los números.
     * Es un DirectChannel para flujo punto a punto.
     */
    @Bean
    fun numberChannel() = MessageChannels.direct()

    /**
     * Canal para números impares.
     * Ahora es Publish-Subscribe, permitiendo múltiples suscriptores.
     * Aun así, el flujo serial se mantiene gracias al filtro y handler final.
     */
    @Bean
    fun oddChannel(): PublishSubscribeChannelSpec<*> = MessageChannels.publishSubscribe()

    /**
     * Canal para números pares.
     * DirectChannel estándar, un único flujo suscriptor.
     */
    @Bean
    fun evenChannel() = MessageChannels.direct()

    /**
     * Flujo principal de polling.
     * Toma números secuenciales del AtomicInteger y los envía al canal central.
     * Se ejecuta cada 100 ms.
     */
    @Bean
    fun pollerFlow(integerSource: AtomicInteger): IntegrationFlow =
        integrationFlow(
            source = { integerSource.getAndIncrement() },
            options = { poller(Pollers.fixedRate(100)) },
        ) {
            channel("numberChannel")
        }

    /**
     * Router de números.
     * Decide el canal de destino según paridad.
     * - Pares → evenChannel
     * - Impares → oddChannel
     * Se registra cada decisión en log.
     */
    @Bean
    fun routerFlow(): IntegrationFlow =
        integrationFlow("numberChannel") {
            transform { num: Int ->
                logger.info("Source generated number: {}", num)
                num
            }
            route { p: Int ->
                val channel = if (p % 2 == 0) "evenChannel" else "oddChannel"
                logger.info("Router: {} → {}", p, channel)
                channel
            }
        }

    /**
     * Flujo de números pares.
     * Transforma el entero a String y registra el procesamiento.
     */
    @Bean
    fun evenFlow(): IntegrationFlow =
        integrationFlow("evenChannel") {
            transform { obj: Int ->
                logger.info("Even Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            handle { p ->
                logger.info("Even Handler: Processed [{}]", p.payload)
            }
        }

    /**
     * Flujo de números impares.
     * Serial: primero filtra impares, luego transforma, luego handler final.
     */
    @Bean
    fun oddFlow(): IntegrationFlow =
        integrationFlow("oddChannel") {
            filter { p: Int ->
                val passes = p % 2 != 0
                logger.info("Odd Filter: checking {} → {}", p, if (passes) "PASS" else "REJECT")
                passes
            }
            transform { obj: Int ->
                logger.info("Odd Transformer: {} → 'Number {}'", obj, obj)
                "Number $obj"
            }
            handle { p ->
                logger.info("Odd Handler: Processed [{}]", p.payload)
            }
        }

    /**
     * Canal para mensajes descartados (opcional, no usado en este flujo).
     */
    @Bean
    fun discarded(): IntegrationFlow =
        integrationFlow("discardChannel") {
            handle { p ->
                logger.info("Discard Handler: [{}]", p.payload)
            }
        }

    /**
     * Tarea programada que envía números aleatorios negativos mediante el gateway.
     * Se ejecuta cada 1000 ms.
     */
    @Scheduled(fixedRate = 1000)
    fun sendNumber() {
        val number = -Random.nextInt(1, 100)
        logger.info("Gateway injecting: {}", number)
        sendNumber.sendNumber(number)
    }
}

/**
 * Messaging Gateway para inyectar números en el flujo.
 * Envía siempre al canal central 'numberChannel'.
 */
@MessagingGateway
interface SendNumber {
    @Gateway(requestChannel = "numberChannel")
    fun sendNumber(number: Int)
}

/**
 * Punto de entrada principal de Spring Boot.
 */
fun main() {
    runApplication<IntegrationApplication>()
}
