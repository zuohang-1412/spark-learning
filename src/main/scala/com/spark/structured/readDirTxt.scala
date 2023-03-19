package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

// 监控 data/monitor_files 文件夹下的文件，获取文件内容。
object readDirTxt {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("read_dir_txt")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    val ds: Dataset[String] = session.readStream.textFile("data/monitor_txt/")
    
    import session.implicits._
    val df: DataFrame = ds.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(0).toInt, arr(1), arr(2).toInt)
    }).toDF("id", "name", "age")
    val query: StreamingQuery = df.writeStream.format("console").start()
    query.awaitTermination()
  }
}
