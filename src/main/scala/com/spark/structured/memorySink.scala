package com.spark.structured

// 将数据结果写入到内存中用于测试
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
object memorySink {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("memory_sink")
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
      .format("memory")
      .queryName("tableName")
      .start()
    while (true) {
      Thread.sleep(5000)
      session.sql( "select * from tableName".stripMargin
      ).show()
    }
    query.awaitTermination()
  }
}
