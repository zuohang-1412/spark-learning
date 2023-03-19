package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object tableAPI {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("table_api")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      // 流表数据会临时存储在 spark-warehouse 中，spark-warehouse是默认文件夹名称
      .config("spark.sql.warehouse.dir", "spark-warehouse/")
      // offset 信息会存储在 checkpoint 中
      .config("spark.sql.streaming.checkpointLocation", "checkpoint/")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    // 创建链接：
    val df: DataFrame = session.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

    import session.implicits._
    // 数据格式 ： 1,zhangSan,22 转换
    val newDF: DataFrame = df.as[String].map(_.split(",")).map(e=>(e(0).toInt, e(1), e(2).toInt)).toDF("id", "name", "age")

    // 数据写入流表
    newDF.writeStream.toTable("userInfo")

    // 查询数据
    val resDF: DataFrame = session.readStream.table("userInfo").select("id", "name", "age")

    // 写入 console 中
    val query: StreamingQuery = resDF.writeStream.format("console").start()
    query.awaitTermination()
  }

}
