package ir.rkr.darkoob


import com.typesafe.config.ConfigFactory
import ir.rkr.darkoob.hbase.HbaseFeeder
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
    JettyRestServer(config, dmetirc)
    val tkFeeder = HbaseFeeder("tk",config,dmetirc)

    logger.info { "Darkoob V$version is ready :D" }

}
