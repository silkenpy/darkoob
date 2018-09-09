package ir.rkr.darkoob


import com.typesafe.config.ConfigFactory
import ir.rkr.darkoob.hbase.HbaseConnector
import ir.rkr.darkoob.kafka.KafkaConnector
import ir.rkr.darkoob.rest.JettyRestServer
import ir.rkr.darkoob.util.DMetric
import mu.KotlinLogging


const val version = 0.1

/**
 * CacheService main entry point.
 */
fun main(args: Array<String>) {
    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val dmetirc = DMetric()
    val kafka = KafkaConnector(config)
    JettyRestServer(kafka, config, dmetirc)
//    val hbase = HbaseConnector("table1",config,dmetirc)
//    hbase.put("personal","name","salam".toByteArray(),"I am Ali".toByteArray())

    logger.info { "Darkoob V$version is ready :D" }


}
