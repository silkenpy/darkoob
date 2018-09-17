package ir.rkr.darkoob


import com.typesafe.config.ConfigFactory
import ir.rkr.darkoob.hbase.HbaseFeeder
import ir.rkr.darkoob.rest.JettyRestServer
import ir.rkr.darkoob.util.DMetric
import mu.KotlinLogging



const val version = 0.2

/**
 * CacheService main entry point.
 */
fun main(args: Array<String>) {
    val logger = KotlinLogging.logger {}
    val config = ConfigFactory.defaultApplication()
    val dmetirc = DMetric()
    JettyRestServer(config, dmetirc)
    val pfFeeder = HbaseFeeder("pf",config,dmetirc)
    val tkFeeder = HbaseFeeder("tk",config,dmetirc)
    val chFeeder = HbaseFeeder("ch",config,dmetirc)
    val k2kFeeder = HbaseFeeder("k2k",config,dmetirc)

    logger.info { "Darkoob V$version is ready :D" }

}
