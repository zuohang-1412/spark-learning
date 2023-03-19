package com.spark.structured

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType

object readDirJson {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("read_dir_json")
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
      .json("data/monitor_json")

    val query: StreamingQuery = df.writeStream.format("console").start()
    query.awaitTermination()
  }

}
