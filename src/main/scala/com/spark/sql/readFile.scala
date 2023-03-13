package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object readFile {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("sql_test")
      .master("local")
//      .enableHiveSupport() // 开启这个选项时，spark on hive
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")
    // DataFrame 就是 DataSet[row]
    val df: DataFrame = session.read.json("data/user_info.txt")
    df.show()
    df.printSchema()

    session.catalog.listDatabases().show()
    session.catalog.listTables().show()
    session.catalog.listFunctions().show()
  }
}
