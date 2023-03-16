package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object socket02 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("socket_02").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    // 在 items 中可以 执行命令 nc -l 8888 来写入数据 ：hello world
    val dataDStream: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.0.101", 8889)
    val mapDStream: DStream[(String, String)] = dataDStream.map(_.split(" ")).map(e => (e(0), e(1)))
    mapDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
