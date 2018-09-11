package ir.rkr.darkoob.hbase

import com.typesafe.config.Config
import ir.rkr.darkoob.kafka.KafkaConnector
import ir.rkr.darkoob.util.DMetric
import kotlin.concurrent.thread

class HbaseFeeder(config: Config, dMetric: DMetric) {

    val kafka = KafkaConnector(config)
    val pfHbase = HbaseConnector("pf", config, dMetric)

    init {
        thread {
            println("HbaseFeeder is initializing !")
            while (true) {
                val msg = kafka.get("pf")

                if (msg.isEmpty()) {
                    Thread.sleep(10)
                } else {

                    msg.forEach { it -> println(String(it.key)+" "+String(it.value)); pfHbase.put(it.key, it.value, "cf", "q") }
                }
            }

        }
    }
    fun start(){
        println("HbaseFeeder is Started !")
    }
}