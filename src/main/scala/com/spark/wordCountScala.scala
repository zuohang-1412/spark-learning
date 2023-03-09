package com.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wordCount")
    conf.setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("Error")
    val fileRDD: RDD[String] = sc.textFile("data/Please_send_me_a_card.txt")
    val wordsRDD: RDD[String] = fileRDD.flatMap( _.split(" "))
    val mapRDD: RDD[(String, Int)] = wordsRDD.map((_, 1))
    val rbkRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _).sortBy(_._2,false)
    rbkRDD.foreach(println)
  }
}
