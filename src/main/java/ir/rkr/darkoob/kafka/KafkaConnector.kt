package ir.rkr.darkoob.kafka


import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord


class KafkaConnector(val config: Config) {


    val consumer:KafkaConsumer<String,String>
    val producer:KafkaProducer<String,String>

    val topicName = config.getString("kafka.consumer.topic.name")
    init {


        val consumercfg = Properties()
        consumercfg.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer" )
        consumercfg.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

        config.getObject("kafka.consumer").forEach({x,y -> println("$x --> $y"); consumercfg.put(x,y.unwrapped())})
        consumer = KafkaConsumer(consumercfg)
        consumer.subscribe(Collections.singletonList(topicName))


        val  producercfg= Properties()
        producercfg.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer" )
        producercfg.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer" )
        config.getObject("kafka.producer").forEach({x,y -> println("$x --> $y"); producercfg.put(x,y.unwrapped())})
        producer = KafkaProducer(producercfg)

    }


   fun get(){
       val res = consumer.poll(2000)
       res.records(topicName).forEach { it -> println(it)}
       consumer.commitAsync()
   }

    fun put(key:String, value:String){
        producer.send(ProducerRecord(topicName,key,value))
    }

}