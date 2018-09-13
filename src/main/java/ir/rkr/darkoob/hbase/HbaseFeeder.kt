package ir.rkr.darkoob.hbase

import com.typesafe.config.Config
import ir.rkr.darkoob.kafka.KafkaConnector
import ir.rkr.darkoob.util.DMetric
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class HbaseFeeder(tableName: String, config: Config, dMetric: DMetric) {

    val Kafka = KafkaConnector(tableName, config)
    val Hbase = HbaseConnector(tableName, config, dMetric)

    init {
        println("HbaseFeeder for $tableName is Started !")
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay({

            val msg = Kafka.get()
            if (msg.isEmpty()) return@scheduleWithFixedDelay

            msg.forEach { it ->

                println(String(it.key) + " " + String(it.value));
                Hbase.put(it.key, it.value, "cf", "q")
                Kafka.commit()
            }
        }, 0, 100, TimeUnit.MILLISECONDS)
    }


}