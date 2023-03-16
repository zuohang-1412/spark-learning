package com.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import java.util


object streamingKafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("streaming_on_kafka").setMaster("local[3]")
    // 反压： 就是对上一次拉取进行压力测试，动态调节拉取量
    conf.set("spark.streaming.backpressure.enabled", "true")
    // 每次拉取数据
    conf.set("spark.streaming.kafka.maxRatePerPartition", "2") //运行时状态
    // 最后退出时要执行完成后再退出
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")


    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc: StreamingContext = new StreamingContext(sc, Duration(1000))

    val map: Map[String, Object] = Map[String, Object](
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"),
      (ConsumerConfig.GROUP_ID_CONFIG, "streamingGroup1"),
//      (ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1") // 在这里设置 每次拉取数量 是无法生效的
    )


    // 如何得到kafka 中的DStream
    val dataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("spark"), map)
    )
    // 使用 KafkaUtils 得到的 DStream 数据需要转换， DStream同比为records 是一个集合。所以执行：dataDS.print()会报错。
    // 需要转换为record 只提取 key和value
    val data: DStream[(String, (String, Int, Long, String))] = dataDS.map(
      record => {
        val topic: String = record.topic()
        val partition: Int = record.partition()
        val offset: Long = record.offset()
        val key: String = record.key()
        val value: String = record.value()

        (key, (topic, partition, offset, value))
      }
    )
    data.print()

    // 完成业务代码后 进行手动维护offset
    dataDS.foreachRDD(
      rdd=>{
        // driver 端可以拿到offset
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 闭包， 通过kafka utils 得到第一个向上转型， 提交offset
        // Async 为异步更新 所以需要一个回调 回调会有延迟
        dataDS.asInstanceOf[CanCommitOffsets].commitAsync(ranges, new OffsetCommitCallback {
          override def onComplete(map: util.Map[TopicPartition, OffsetAndMetadata], e: Exception): Unit = {
            if(map != null) {
              ranges.foreach(println)
              println("---------------")
              val iter: util.Iterator[TopicPartition] = map.keySet().iterator()
              while (iter.hasNext) {
                val k: TopicPartition = iter.next()
                val v: OffsetAndMetadata = map.get(k)
                println(s"${k.partition()}....${v.offset()}")
              }
            }
          }
        })
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }
}
