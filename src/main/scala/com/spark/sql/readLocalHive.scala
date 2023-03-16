package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

// 要config("hive.metastore.uris", "thrift://127.0.0.1:9083") 成功，需要启动：
// hive --service hiveserver2
// hive --service metastore
object readLocalHive {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("localHive")
      .master("local")
      .config("hive.metastore.uris", "thrift://127.0.0.1:9083")
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    import session.implicits._
    val df: DataFrame = List(
      "zhangSan",
      "liSi",
      "wangWu"
    ).toDF("name")
//    df.createTempView("userInfo") // 1. 这个时候为临时表，会存储在缓存中，并不会在hive中
//    df.write.saveAsTable("userInfo") // 2. 这时才会在hive中
//    session.sql("insert into userInfo values ('maLiu')") // 3. 追加数据， 如果出现加入不成功的问题，那可能是spark 无法直接与hdfs建立访问
    session.sql("select * from userInfo").show()

    session.catalog.listDatabases().show()
    session.catalog.listTables().show()

  }
}
