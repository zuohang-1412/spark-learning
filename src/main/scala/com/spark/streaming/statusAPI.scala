package com.spark.streaming

import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object statusAPI {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("status_API").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(1))
    val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val mapDS: DStream[(String, Int)] = data.map(_.split(" ")).map(e => (e(0), 1))
    // 持久化 目录 正常会写 hdfs://****
    sc.setCheckpointDir(".")

    //  new + old 求和
/*    val res: DStream[(String, Int)] = mapDS.updateStateByKey((newV: Seq[Int], oldV: Option[Int]) => {

      // 每个job 中 对new 进行求个数
      val count: Int = newV.count(_ > 0)
      // 获取 oldV 中的值
      val oldVal: Int = oldV.getOrElse(0)
      Some(count + oldVal)

    })
    res.print()*/

    // updateStateByKey 已经过时， 因为 newV 是通过 seq[Int] 进行加和计算的， 如果seq 中数据量过大，就会导致内存溢出的情况发生。
    // 所以采用新的 API mapWithState 做全量有
    val stateDS: MapWithStateDStream[String, Int, Int, (String, Int)] = mapDS.mapWithState(StateSpec.function(
      (k: String, newV: Option[Int], oldV: State[Int]) => {
        println(s"k:${k}  newV:${newV.getOrElse(0)}  oldV:${oldV.getOption().getOrElse(0)}")
        val vv: Int = newV.getOrElse(0) + oldV.getOption().getOrElse(0)
        oldV.update(vv)
        (k, vv)
      }
    ))
    stateDS.print()

    // reduceByKeyAndWindow 调优 当窗口大小大于步长时， 可以利用加入进来的，减去出去的
    // reduceByKeyAndWindow 对  combinationBykey的封装  放入函数，聚合函数，combina函数
/*    val res51Batch_3: DStream[(String, Int)] = mapDS.reduceByKeyAndWindow(
      // 加上新进来的 batch 数据 【为聚合函数 即有两条的时候才会调取】
      (ov: Int, nv: Int) => {
        nv + ov
      },
      // 减去出去的 batch 数据
      (ov: Int, oov: Int) => {
        ov - oov
      },
      Duration(5000),
      Duration(2000)
    )
    // 打印频次
    res51Batch_3.print()*/


    ssc.start()
    ssc.awaitTermination()
  }
}
