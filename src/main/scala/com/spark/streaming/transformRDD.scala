package com.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object transformRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("transform_RDD").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc: StreamingContext = new StreamingContext(sc, Duration(1000))
    val res: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val formatDS: DStream[(String, Int)] = res.map(_.split(" ")).map(e => (e(0), e(1).toInt))

    // transform 中途加工  RDD操作 数值放大100倍
    val newDS: DStream[(String, Int)] = formatDS.transform(
      (rdd) => {
        rdd.map(x => (x._1, x._2 * 100))
      }
    )
    newDS.print()

    // foreachRDD 末端处理 里面一定要有action算子
    formatDS.foreachRDD(
      (rdd)=>{
        rdd.foreach(print)
      }
    )



    ssc.start()
    ssc.awaitTermination()
  }
}
