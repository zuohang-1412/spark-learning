package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object socket01 {
  def main(args: Array[String]): Unit = {
    // 不能写local  必须大于1个线程才能执行
    val conf: SparkConf = new SparkConf().setAppName("socket_01").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    // 在 items 中可以 执行命令 nc -l 8888 来写入数据 ：hello world
    val dataDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.0.101", 8888)
    val flatDStream: DStream[String] = dataDStream.flatMap(_.split(" "))
    // 在 批次（5秒）内的聚合
    val resDStream: DStream[(String, Int)] = flatDStream.map((_, 1)).reduceByKey(_+_)
    resDStream.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
