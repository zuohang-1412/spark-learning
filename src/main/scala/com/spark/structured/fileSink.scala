package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}


// 将数据写出到文件， 文件格式为 csv、json、orc、parquet、text
// 写出的文件必须指定 checkoutpoint 文件夹位置
object fileSink {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("file_sink")
      .master("local")
      // 设置并行度
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    // 读取数据:  nc -l 9999
    val df: DataFrame = session.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 在路径中生成csv 文件
    val query: StreamingQuery = df.writeStream
      .format("csv")
      .option("path", "data/sink_dir")
      .option("checkpointLocation", "checkpoint")
      .start()
    query.awaitTermination()
  }
}
