package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

// foreachSink foreachBatch, 一批次进行保存， 自定义保存数据
object foreachBatchSink {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("foreachBatch_sink")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    val df: DataFrame = session
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import session.implicits._
    val userDF: DataFrame = df.as[String].map(
      e => {
        val arr: Array[String] = e.split(",")
        (arr(0).toInt, arr(1), arr(2).toInt)
      }
    ).toDF("id", "name", "age")

    val query: StreamingQuery = userDF.writeStream.foreachBatch(
      (batchDF: DataFrame, batchID: Long) => {
        println("batchID:", batchID)
        // 批次数据 batchDF
        batchDF.write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/data")
          .option("user", "root")
          .option("password", "zuohang123")
          .option("dbtable", "foreachBatchSink")
          .save()
      }
    ).start()
    query.awaitTermination()
  }
}
