package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object readHive {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("read_hive")
      .master("local")
      .config("spark.sql.partitions", 1)
//      .config("spark.sql.warehouse.dir", "/")  可以不用默认路径
      .enableHiveSupport() // hive 支持
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    session.sql("create database IF NOT EXISTS spark") // 作用在 default 库中
    session.sql("create table IF NOT EXISTS users1 (id int, name string, age int)") // 这个是 default 中的 users1
    session.catalog.listTables().show() // 作用在 default 库 中
    session.sql("use spark") // 作用在 spark 库
    session.sql("create table IF NOT EXISTS users2 (id int, name string, age int)") // 这个是 spark 中的 users2
    session.sql("insert into default.users1 values  (1, \"zhangsan\", 18), (2, \"lisi\",\n19), (3,\"wangwu\", 20)")
    session.sql("insert into spark.users2 values  (1, \"zhangsan\", 18), (2, \"lisi\",\n19), (3,\"wangwu\", 20)")
    session.catalog.listTables().show() // 作用在 spark 库 中
  }
}
