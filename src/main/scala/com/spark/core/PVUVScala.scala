package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PVUVScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sort_scala")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val fileRDD: RDD[String] = sc.textFile("data/event.txt", 5)

    // 各网站 PV、UV 前五名
    // 43.169.217.152	河北	2018-11-12	1542011088714	3292380437528494072	www.dangdang.com	Login

    // PV
    println("----PV----")
    val lineRDD: RDD[(String, Int)] = fileRDD.map((line: String) => {
      val lineList: Array[String] = line.split("\t")
      (lineList(5), 1)
    })
    val pvRDD: RDD[(String, Int)] = lineRDD.reduceByKey(_ + _).sortBy(_._2, false)
    val PVRes: Array[(String, Int)] = pvRDD.take(5)
    PVRes.foreach(println)

    // UV
    println("----UV----")
    val usersRDD: RDD[(String, String)] = fileRDD.map((line: String) => {
      val lineList: Array[String] = line.split("\t")
      (lineList(5), lineList(0))
    }).distinct()
    val onceRDD: RDD[(String, Int)] = usersRDD.map((line: (String, String)) => {
      (line._1, 1)
    })
    val uvRDD: RDD[(String, Int)] = onceRDD.reduceByKey(_+_).sortBy(_._2,false)
    val UVRes: Array[(String, Int)] = uvRDD.take(5)
    UVRes.foreach(println)
    Thread.sleep(Long.MaxValue)
  }
}
