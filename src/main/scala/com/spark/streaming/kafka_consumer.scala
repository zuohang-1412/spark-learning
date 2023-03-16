package com.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

/*
* kafka-consumer
* 1. 自动维护offset：AUTO_OFFSET_RESET_CONFIG = true  自动维护过程为： 先poll 数据 然后更改offset 再去计算， 如果挂了就会出现 丢失数据的情况。
* 2. 手动维护offset：AUTO_OFFSET_RESET_CONFIG = false
*   1) 维护到kafka 自己的 __consumer_offset_ 中， 可以通过 kafka-consumer-groups.sh 进行查看更新的位置。
*   2）维护到zk、或mysql 中 这个时候就需要运用 ConsumerRebalanceListener 进行 seek 更新offset
*
* */

object kafka_consumer {
  def main(args: Array[String]): Unit = {
    val pros: Properties = new Properties()
    pros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    // earliest CURRENT-OFFSET 特殊状态：group 第一次创建的时候为0
    // latest LOG-END-OFFSET
    pros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "TRUE")
    pros.put(ConsumerConfig.GROUP_ID_CONFIG, "first_group")

    val consumer = new KafkaConsumer[String, String](pros)
    // 订阅 topic 为spark
    consumer.subscribe(Pattern.compile("spark"), new ConsumerRebalanceListener {
      override def onPartitionsRevoked(collection: util.Collection[TopicPartition]): Unit = {
        println("onPartitionsRevoked")
        val iter: util.Iterator[TopicPartition] = collection.iterator()
        while (iter.hasNext) {
          println(iter.next())
        }

      }

      override def onPartitionsAssigned(collection: util.Collection[TopicPartition]): Unit = {
        println("onPartitionsAssigned")
        val iter: util.Iterator[TopicPartition] = collection.iterator()
        while (iter.hasNext) {
          println(iter.next())
        }

        // seek 分区1 offset 22 从22 开始读取
        consumer.seek(new TopicPartition("spark", 1), 22)
        Thread.sleep(5000)
      }
    })
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(0))
      var record: ConsumerRecord[String, String] = null

      if (!records.isEmpty) {
        println(s"----${records.count()}----")
        val iter: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        while (iter.hasNext) {
          record = iter.next()
          val topic: String = record.topic()
          val partition: Int = record.partition()
          val offset: Long = record.offset()
          val key: String = record.key()
          val value: String = record.value()
          println(s"key: $key  value:$value  topic:$topic  partition:$partition  offset:$offset")

        }
        // 维护offset 的目的是防止丢失数据
        // 手动维护到kafka ENABLE_AUTO_COMMIT_CONFIG =  "false"
        // 维护到mysql 或 zk 中
        val partition: TopicPartition = new TopicPartition("spark", record.partition())
        val offset: OffsetAndMetadata = new OffsetAndMetadata(record.offset())
        val offMap: util.HashMap[TopicPartition, OffsetAndMetadata] = new util.HashMap[TopicPartition, OffsetAndMetadata]()
        offMap.put(partition, offset)
        consumer.commitSync(offMap)
      }
    }
  }
}
