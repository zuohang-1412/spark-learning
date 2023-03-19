package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}

object Count60Students {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("count_students")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    val schema: StructType = new StructType()
      .add("name", DataTypes.StringType)
      .add("class", DataTypes.StringType)
      .add("score", DataTypes.IntegerType)
    val df: DataFrame = session
      .readStream
      .schema(schema)
      .option("seq", ",")
      .csv("data/monitor_score")
    df.createTempView("student_score")
    val resDF: DataFrame = session.sql("select class, count(1) as cnt, avg(score) as avg_score from student_score where score>=60 group by class".stripMargin)
    val query: StreamingQuery = resDF.writeStream.format("console").outputMode("complete").start()
    query.awaitTermination()
  }
}
