package com.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object windowAPI {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("window_API").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir(".")
    // Duration(1000) = Seconds(1)  1秒
    val ssc: StreamingContext = new StreamingContext(sc, Duration(1000))

    // 因为上面设置的是1秒， 下面的操作一定是大于等于1 秒的操作， 没法做更细力度的操作。
    val res: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val formatDS: DStream[(String, Int)] = res.map(_.split(" ")).map(e => (e(0), 1))

//    // 1秒计算
//    val res1Batch: DStream[(String, Int)] = formatDS.reduceByKey(_ + _)
//    // 打印频次
//    res1Batch.mapPartitions(iter=>{print("1S");iter}).print()
//
//    // 历史5秒计算
//    // window 窗口大小： 5秒 ； 步进大小：5秒
//    val formatDS5: DStream[(String, Int)] = formatDS.window(Duration(5000), Duration(5000))
//    val res5Batch: DStream[(String, Int)] = formatDS5.reduceByKey(_ + _)
//    // 打印频次
//    res5Batch.mapPartitions(iter=>{print("5S");iter}).print()
//
//    // 每秒打印前5秒的统计
//    val formatDS51: DStream[(String, Int)] = formatDS.window(Duration(5000), Duration(1000))
//    val res51Batch: DStream[(String, Int)] = formatDS51.reduceByKey(_ + _)
//    // 打印频次
//    res51Batch.mapPartitions(iter=>{print("5S");iter}).print()
//
//    // 每秒打印前5秒的统计
//    val res51Batch_2: DStream[(String, Int)] = formatDS.reduceByKeyAndWindow(_+_, Duration(5000))
//    // 打印频次
//    res51Batch_2.mapPartitions(iter => {
//      print("5S"); iter
//    }).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
