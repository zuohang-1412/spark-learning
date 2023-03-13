package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object S_RDD2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("rdd2_scala").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val dataRDD: RDD[(String, Int)] = sc.parallelize(List(
      ("ZhangSan", 80),
      ("ZhangSan", 70),
      ("ZhangSan", 98),
      ("LiSi", 60),
      ("LiSi", 76),
      ("LiSi", 80),
      ("WangWu", 50),
      ("WangWu", 30),
      ("WangWu", 88)
    ))

    // 列转行
    println("----列转行----")
    val grRDD: RDD[(String, Iterable[Int])] = dataRDD.groupByKey()
    grRDD.foreach(println)

    // 行转列
    println("----行转列----")
    val lineRDD: RDD[(String, Int)] = grRDD.flatMap((str: (String, Iterable[Int])) => {
      str._2.map((x: Int) => {
        (str._1, x)
      }).iterator
    })
    lineRDD.foreach(println)

    // 行转列 第二种
    println("----行转列 第二种----")
    val line2RDD: RDD[(String, Int)] = grRDD.flatMapValues(e => e.iterator)
    line2RDD.foreach(println)

    // 每人分数排序后前两个
    println("----每人分数排序后前两个----")
    val res1RDD: RDD[(String, List[Int])] = grRDD.mapValues(e => e.toList.sorted.take(2))
    val res2RDD: RDD[(String, Int)] = grRDD.flatMapValues(e => e.toList.sorted.take(2))
    println("*************")
    res1RDD.foreach(println)
    println("*************")
    res2RDD.foreach(println)

    // sum count max min
    println("----sum----")
    val sumP: RDD[(String, Int)] = dataRDD.reduceByKey(_ + _)
    sumP.foreach(println)
    println("----max----")
    val maxP: RDD[(String, Int)] = dataRDD.reduceByKey((oldV: Int, newV: Int) => {
      if (oldV < newV) newV else oldV
    })
    maxP.foreach(println)
    println("----min----")
    val minP: RDD[(String, Int)] = dataRDD.reduceByKey((oldV: Int, newV: Int) => {
      if (oldV > newV) newV else oldV
    })
    minP.foreach(println)
    println("----count----")
    val cntP: RDD[(String, Int)] = dataRDD.mapValues(e => 1).reduceByKey(_ + _)
    cntP.foreach(println)
    println("----avg----")
    val avgP: RDD[(String, Int)] = sumP.join(cntP).mapValues((d: (Int, Int)) => {
      d._1 / d._2
    })
    avgP.foreach(println)

    println("----avg 调优----")
    /* combineByKey
    createCombiner: V => C
    mergeValue: (C, V) => C
    mergeCombiners: (C, C) => C*/
    val comRDD: RDD[(String, (Int, Int))] = dataRDD.combineByKey(
      (value: Int) => (value, 1),
      (oldV: (Int, Int), newV: Int) => (oldV._1 + newV, oldV._2 + 1),
      (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2)
    )
    val mapVRDD: RDD[(String, Int)] = comRDD.mapValues(e => e._1 / e._2)
    mapVRDD.foreach(println)


  }
}
