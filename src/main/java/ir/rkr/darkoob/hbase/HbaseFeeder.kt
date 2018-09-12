package ir.rkr.darkoob.hbase

import com.typesafe.config.Config
import ir.rkr.darkoob.kafka.KafkaConnector
import ir.rkr.darkoob.util.DMetric
import java.nio.ByteBuffer
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class HbaseFeeder(config: Config, dMetric: DMetric) {

    val kafka = KafkaConnector(config)
    val pfHbase = HbaseConnector("pf", config, dMetric)

    init {
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

            println("HbaseFeeder is initializing !")

            val msg = kafka.get("pf")
            if (msg.isEmpty()) return@scheduleWithFixedDelay

            msg.forEach { it ->

                println(String(it.key) + " " + String(it.value));
                pfHbase.put(it.key, it.value, "cf", "q")
            }
        }, 0, 100, TimeUnit.MILLISECONDS)
    }

    fun start() {
        println("HbaseFeeder is Started !")
    }
}