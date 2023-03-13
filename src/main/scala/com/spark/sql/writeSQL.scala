package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn._
object writeSQL {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("writeSQL").getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    val df: DataFrame = session.read.json("data/user_info.txt")
    df.createTempView("user_info")
    while (true){
      val sql: String = readLine("input your sql: ")
      session.sql(sql).show()
    }
  }
}
