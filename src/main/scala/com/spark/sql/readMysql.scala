package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object readMysql {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("readMysql")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1) // 可以将默认的 200 并行度，设置为1
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("INFO")
    val pro = new Properties()
    pro.put("url", "jdbc:mysql://localhost:3306/data")
    pro.put("user", "root")
    pro.put("password", "zuohang123")
    pro.put("driver", "com.mysql.cj.jdbc.Driver")
    val usersDF: DataFrame = session.read.jdbc(pro.getProperty("url"), "users", pro)
    val scoresDF: DataFrame = session.read.jdbc(pro.getProperty("url"), "scores", pro)

    usersDF.createTempView("users")
    scoresDF.createTempView("scores")
    val res: DataFrame = session.sql("select a.name, a.age, b.project, b.score from users a join scores b on a.id = b.id")
    res.show()
    // 默认并行度为 200 可以通过设置conf来变更 partitions大小
    println("任务数：",res.rdd.partitions.length)


    // 写的话 要重新规划任务数

//    res.coalesce(1)
//      println("任务数：",res.rdd.partitions)
//    res.write.jdbc()


  }
}
