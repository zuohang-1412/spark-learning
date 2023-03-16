package com.spark.streaming

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.concurrent.Future

object kafka_producer {
  def main(args: Array[String]): Unit = {
    val pros: Properties = new Properties()
    pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](pros)

    while(true) {
      for (i<- 1 to 3; j<- 1 to 3) {
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("spark", s"item$j", s"action$i")
        val records: Future[RecordMetadata] = producer.send(record)
        val metadata: RecordMetadata = records.get()
        val partition: Int = metadata.partition()
        val offset: Long = metadata.offset()
        println(s"item:$j action:$i partition:$partition offset:$offset")
      }
      Thread.sleep(1000)
    }
  }

}
