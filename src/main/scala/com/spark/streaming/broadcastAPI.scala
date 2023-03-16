package com.spark.streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object broadcastAPI {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))
    val res: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val mapDS: DStream[(String, Int)] = res.map(_.split(" ")).map(e => (e(0), e(1).toInt))

    // 作用域分为三个级别
    // application 只打印一次
    println("aaaaaa")
    val zyy: DStream[(String, Int)] = mapDS.transform(
      (rdd) => {
        // job 每个job 执行一次
        println("bbbbbb")
        rdd.map(x => {
          // rdd task 每条数据执行一次
          println("cccccc")
          x
        })
      }
    )
    zyy.print()


    // 广播变量
    var bc: Broadcast[List[Int]] = null
    var jobNum: Int = 0
    // 只打印 1至5
    val resDS: DStream[(String, Int)] = mapDS.transform(
      rdd => {
        jobNum += 1
        println(s"jobNum: $jobNum")
        // 中间因为过滤就会有一些job 中不会有数据
        if (jobNum <= 5) {
          bc = sc.broadcast((1 to 5).toList)
        } else {
          bc = sc.broadcast((10 to 15).toList)
        }
        rdd.filter(x => {
          bc.value.contains(x._2)
        })
      }
    )

    resDS.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
