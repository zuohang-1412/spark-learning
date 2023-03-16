package com.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object receive01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("receive01").setMaster("local[9]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")

    val dstream: ReceiverInputDStream[String] = ssc.receiverStream(new CustormReceiver("localhost", 8889))

    dstream.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
