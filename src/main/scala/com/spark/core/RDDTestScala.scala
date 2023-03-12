package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTestScala {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("rdd_test_scala").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Error")
    val dataRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1))
    //  过滤
    println("----过滤----")
    val filterRDD: RDD[Int] = dataRDD.filter(_ > 3)
    filterRDD.foreach(println)

    // 去重
    println("----去重----")
    val setRDD: RDD[Int] = dataRDD.map((_, 1)).reduceByKey(_ + _).map(_._1)
    val disRDD: RDD[Int] = dataRDD.distinct()
    setRDD.foreach(println)
    disRDD.foreach(println)

    // 并集 窄依赖 RangeDependency
    println("----并集----")
    val v1RDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    val v2RDD: RDD[Int] = sc.parallelize(List(6, 7, 8, 9, 10))
    val v3RDD: RDD[Int] = sc.parallelize(List(3, 4, 5, 6, 7, 8))
    println("v1RDD size："+ v1RDD.partitions.size)
    println("v1RDD size："+ v2RDD.partitions.size)
    val unionRDD: RDD[Int] = v1RDD.union(v2RDD)
    println("unionRDD size："+ unionRDD.partitions.size)
    unionRDD.foreach(println)

    // 交集
    println("----交集----")
    val interRDD: RDD[Int] = v3RDD.intersection(v1RDD)
    interRDD.foreach(println)

    // 差集
    println("----差集----")
    val subRDD: RDD[Int] = v3RDD.subtract(v1RDD)
    subRDD.foreach(println)

    // 笛卡尔积 窄依赖
    println("----笛卡尔积----")
    val cartesianRDD: RDD[(Int, Int)] = v1RDD.cartesian(v2RDD)
    cartesianRDD.foreach(println)

    // cogroup 可以窄依赖 可以宽依赖
    println("----cogroup----")
    val kv1RDD: RDD[(String, Int)] = sc.parallelize(List(("ZhangSan", 18), ("ZhangSan", 19), ("LiSi", 20), ("WangWu", 21)))
    val kv2RDD: RDD[(String, Int)] = sc.parallelize(List(("ZhangSan", 38), ("ZhangSan", 39), ("LiSi", 10), ("MaLiu", 51)))
    val coRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = kv1RDD.cogroup(kv2RDD)
    coRDD.foreach(println)

    // 内关联
    println("----内关联----")
    val joinRDD: RDD[(String, (Int, Int))] = kv1RDD.join(kv2RDD)
    joinRDD.foreach(println)

    // 左关联
    println("----左关联----")
    val leftRDD: RDD[(String, (Int, Option[Int]))] = kv1RDD.leftOuterJoin(kv2RDD)
    leftRDD.foreach(println)

    // 右关联
    println("----右关联----")
    val rightRDD: RDD[(String, (Option[Int], Int))] = kv1RDD.rightOuterJoin(kv2RDD)
    rightRDD.foreach(println)

    // 全关联
    println("----全关联----")
    val fullRDD: RDD[(String, (Option[Int], Option[Int]))] = kv1RDD.fullOuterJoin(kv2RDD)
    fullRDD.foreach(println)
  }
}
