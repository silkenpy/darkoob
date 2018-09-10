package ir.rkr.darkoob.kafka


import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

class KafkaConnector(val topicName: String, config: Config) {


    val consumer: KafkaConsumer<ByteArray, ByteArray>
    val producer: KafkaProducer<ByteArray, ByteArray>
//    val topicName = config.getString("kafka.consumer.topic.name")

    init {


        val consumercfg = Properties()
        consumercfg.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
        consumercfg.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")

        config.getObject("kafka.consumer").forEach({ x, y -> println("kafka config $x --> $y"); consumercfg.put(x, y.unwrapped()) })
        consumer = KafkaConsumer(consumercfg)
        consumer.subscribe(Collections.singletonList(topicName))


        val producercfg = Properties()
        producercfg.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        producercfg.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        config.getObject("kafka.producer").forEach({ x, y -> println("$x --> $y"); producercfg.put(x, y.unwrapped()) })
        producer = KafkaProducer(producercfg)

    }


    fun get(): ByteArray {
        val res = consumer.poll(2000)
        //res.records(topicName).forEach { it ->  return it.value()}
        return res.records(topicName).first().value()
//       consumer.commitAsync()
    }

    fun put(key: ByteArray, value: ByteArray) {
        producer.send(ProducerRecord(topicName, key, value))
    }

}