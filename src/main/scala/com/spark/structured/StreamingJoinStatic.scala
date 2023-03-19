package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object StreamingJoinStatic {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("streaming_join_static")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val list = List[String]("{\"id\":1, \"project\": \"chinese\"}",
      "{\"id\":2, \"project\": \"math\"}",
      "{\"id\":3, \"project\": \"english\"}",
    ).toDS()

    val staticDF: DataFrame = spark.read.json(list)

    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    val scoreInfo: DataFrame = df.as[String].map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0), arr(1).toInt, arr(2)toInt)
    }).toDF("name", "proId", "score")


    // 支持 full join； left join； left semi join； 不支持right join
    val result: DataFrame = scoreInfo.join(staticDF, scoreInfo.col("proId") === staticDF.col("id"), "left")
    result.printSchema()

    val query: StreamingQuery = result.writeStream.format("console").start()
    query.awaitTermination()
  }
}
