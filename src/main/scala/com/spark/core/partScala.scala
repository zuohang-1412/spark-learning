package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object partScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("partitions_scala") setMaster ("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val dataRDD: RDD[Int] = sc.parallelize(1 to 10, 2)

    // map
    println("----map----")
    val mapRDD: RDD[String] = dataRDD.map(
      (v: Int) => {
        println("---conn---mysql")
        println(s"----select $v---")
        println("close mysql")
        v + "selected"
      }
    )
    mapRDD.foreach(println)

    // mapPartitionsWithIndex
    println("----mapPartitionsWithIndex----")
    val mppRDD: RDD[String] = dataRDD.mapPartitionsWithIndex(
      (pindex, piter) => {
        // ListBuffer 是一个致命的缺点，因为一般spark 运行是靠 iterator 进行 pipeline 操作， 所以不会占用太多缓存。
        // 但是如果要是应用 ListBuffer 就会造成数据积压，内存溢出的情况。
        val lb: ListBuffer[String] = new ListBuffer[String]
        println(s"$pindex--conn--mysql")
        while (piter.hasNext) {
          val v: Int = piter.next()
          println(s"----$pindex--select $v---")
          lb.+=(v + "selected")
        }
        println("close mysql")
        lb.iterator
      }
    )
    mppRDD.foreach(println)

    // mapPartitionsWithIndex 优化
    println("----mapPartitionsWithIndex 优化----")
    val newmppRDD: RDD[String] = dataRDD.mapPartitionsWithIndex(
      (pindex, piter) => {
        new Iterator[String] {
          println(s"$pindex--conn--mysql")

          override def hasNext = if (piter.hasNext == false) {
            println(s"$pindex--close--mysql"); false
          } else true

          override def next() = {
            val value: Int = piter.next()
            println(s"----$pindex--select $value---")
            value + "selected"
          }
        }
      }
    )
    newmppRDD.foreach(println)
  }
}
