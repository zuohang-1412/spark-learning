package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object sqlAPI {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("SQL_API").master("local").getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")
    import  session.implicits._
    val DataDF = List(
      "hello world",
      "hello world",
      "hello spark",
      "hello Hadoop",
      "hello world",
      "hello Java",
      "hello spark",
      "hello Hadoop",
      "hello spark",
      "hello spark",
    ).toDF("line")

    // 第一种 求单词个数 正常查询
    println("---- 第一种 ----")
    DataDF.createTempView("talk")
    val df: DataFrame = session.sql(" select word, count(1) from (select explode(split(line, ' ')) as word from talk)t group by t.word ")
    df.show()
    df.printSchema()

    // 第二种 API形式
    println("---- 第二种 ----")
    val df2: DataFrame = DataDF.selectExpr("explode(split(line, ' ')) as word").groupBy("word").count()
    df2.show()
    df2.printSchema()
  }
}
