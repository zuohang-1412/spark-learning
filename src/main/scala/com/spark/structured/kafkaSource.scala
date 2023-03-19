package com.spark.structured

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object kafkaSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("kafaka_source")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark")
      .load()
    df.printSchema()

    import spark.implicits._
    // 数据格式： key：    value：A，B，C
    val words: DataFrame = df.selectExpr("cast (key as string)", "cast (value as string)")
      .as[(String, String)]
      .flatMap(e=> {
        e._2.split(",")
      }).toDF("word")
      .groupBy("word").count()

    val result: Dataset[String] = words.map(row => {
      val word: String = row.getString(0)
      val cnt: Long = row.getLong(1)
      word + "," + cnt
    })

    val query: StreamingQuery = result.writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("1 minutes"))
      .start()
    query.awaitTermination()
  }
}
