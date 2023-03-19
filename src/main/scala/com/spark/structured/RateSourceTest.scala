package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object RateSourceTest {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("rate_source")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    val df: DataFrame = session.readStream
      .format("rate")
      // 每秒生成 n 行 数据
      .option("rowsPerSecond", 10)
      // 设置并行度
      .option("numPartitions", 5)
      .load()
    val query: StreamingQuery = df.writeStream
      .format("console")
      .option("numRows", 100)
      .option("truncate", "false")
      .start()
    query.awaitTermination()
  }
}
