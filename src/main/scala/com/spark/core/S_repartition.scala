package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object S_repartition {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("repartition_scala").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val dataRDD: RDD[Int] = sc.parallelize(1 to 6, 3)

    // 原始分区
    println(s"dataRDD partitions:${dataRDD.getNumPartitions}")
    val partRDD: RDD[(Int, Int)] = dataRDD.mapPartitionsWithIndex(
      (pindex, pitera) => {
        pitera.map(e => (pindex, e))
      }
    )
    partRDD.foreach(println)

    println("*********")
    // repartition 均匀的把之前分区的内容打散放入新分区中
    val newRDD: RDD[(Int, Int)] = partRDD.repartition(2)
    val repartRDD: RDD[(Int, (Int, Int))] = newRDD.mapPartitionsWithIndex(
      (pindex, pitera) => {
        pitera.map(e => (pindex, e))
      }
    )
    println(s"newRDD partitions:${repartRDD.getNumPartitions}")
    repartRDD.foreach(println)

  }
}
