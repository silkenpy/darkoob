package ir.rkr.darkoob.kafka


import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*
import javax.print.DocFlavor
import kotlin.collections.HashMap


/**
 * [Results] is a data model for responses.
 */
data class Results(var results: HashMap<ByteArray, ByteArray> = HashMap<ByteArray, ByteArray>())

class KafkaConnector( config: Config) {


    val consumer: KafkaConsumer<ByteArray, ByteArray>
    val producer: KafkaProducer<ByteArray, ByteArray>
//    val topicName = config.getString("kafka.consumer.topic.name")

    init {


        val consumercfg = Properties()
        consumercfg.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        consumercfg.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

        config.getObject("kafka.consumer").forEach({ x, y -> println("kafka config $x --> $y"); consumercfg.put(x, y.unwrapped()) })
        consumer = KafkaConsumer(consumercfg)

        val producercfg = Properties()
        producercfg.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        producercfg.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        config.getObject("kafka.producer").forEach({ x, y -> println("$x --> $y"); producercfg.put(x, y.unwrapped()) })
        producer = KafkaProducer(producercfg)

        Thread.sleep(100)

    }

    fun get(topicName:String): Map<ByteArray,ByteArray> {
        consumer.subscribe(Collections.singletonList(topicName))
        val res = consumer.poll(2000)
        val msg = HashMap<ByteArray,ByteArray>()
        res.records(topicName).forEach { it ->  msg[it.key()]=it.value()}
        consumer.commitAsync()
        return msg
    }


    fun put(topicName:String,key: ByteArray, value: ByteArray) {
        producer.send(ProducerRecord(topicName, key, value))
    }

}