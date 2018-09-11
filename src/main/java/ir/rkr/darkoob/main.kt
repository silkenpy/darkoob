package ir.rkr.darkoob


import com.typesafe.config.ConfigFactory
import ir.rkr.darkoob.hbase.HbaseConnector
import ir.rkr.darkoob.hbase.HbaseFeeder
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
    JettyRestServer(config, dmetirc)
    val feeder = HbaseFeeder(config,dmetirc)
    feeder.start()
    logger.info { "Darkoob V$version is ready :D" }

    //    val hbase = HbaseConnector("pf",config,dmetirc)
//    hbase.put("salam".toByteArray(),"I am Ali".toByteArray(),"cf","q")
    //  val res = hbase.get("salam".toByteArray(),"cf","q")
//    val res = hbase.scan("salam".toByteArray(),"salam".toByteArray())
//    println(res)


}
