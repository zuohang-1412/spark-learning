package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object S_sample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sample_scala").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val dataRDD: RDD[Int] = sc.parallelize(1 to 100)
    println("----------")
    // 第一个参数：是否重复抽取 false 不重复 true 可以重复
    // 第二个参数：抽取样本量 0.1 即10%
    // 第三个参数：种子，如果两个sample 种子相同则抽取数据一样（一般不写）
    dataRDD.sample(false,0.1).foreach(println)
    println("----------")
    dataRDD.sample(false,0.1).foreach(println)
    println("----------")
    dataRDD.sample(false,0.1).foreach(println)

  }
}
