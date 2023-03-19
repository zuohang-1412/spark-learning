package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

object readDirCsv {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("read_dir_csv")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    val userSchema: StructType = new StructType()
      .add("id", "integer")
      .add("name", "string")
      .add("age", "integer")

    val df: DataFrame = session.readStream
      .schema(userSchema)
      .option("seq", ",")
      .csv("data/monitor_csv")

    val query: StreamingQuery = df.writeStream.format("console").start()
    query.awaitTermination()
  }
}
