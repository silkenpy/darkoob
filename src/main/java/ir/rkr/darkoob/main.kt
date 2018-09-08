package ir.rkr.darkoob


import com.typesafe.config.ConfigFactory
import ir.rkr.darkoob.kafka.KafkaConnector
import ir.rkr.darkoob.rest.JettyRestServer
import ir.rkr.darkoob.util.LayeMetrics
import mu.KotlinLogging


const val version = 0.1

/**
 * CacheService main entry point.
 */
fun main(args: Array<String>) {
    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val layemetrics = LayeMetrics()
    val kafka = KafkaConnector(config)
    JettyRestServer(kafka, config, layemetrics)

    logger.info { "Laye V$version is ready :D" }

}
