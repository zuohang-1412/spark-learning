package com.spark.structured

import java.sql.Timestamp
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}
import org.apache.spark.sql.{DataFrame, SparkSession}

object streamJoinStream {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("stream_join_stream")
      .appName("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val df1: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9998)
      .load()
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (new Timestamp(arr(0).toLong), arr(1).toInt, arr(2), arr(3).toInt)
      }).toDF("ats", "aid", "aname", "age")
      .withWatermark("ats", "3 seconds")
      .dropDuplicates("aid","aname")

    //设置第二个流
    val df2: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (new Timestamp(arr(0).toLong), arr(1).toInt, arr(2), arr(3).toInt)
      }).toDF("bts", "bid", "bname", "score")
      .withWatermark("bts", "5 seconds")
      // 流去重
      .dropDuplicates("bid","bname")


    //两个流进行关联
    import org.apache.spark.sql.functions._
    val result = df1.join(df2, expr(
      """
        | aid = bid and
        | bts >= ats and
        | bts <= ats + interval 10 seconds
         """.stripMargin
    ), "leftOuter")


    val query1: StreamingQuery = df1.writeStream
      .format("console")
      .queryName("query1")
      .start()

    val query2: StreamingQuery = df2.writeStream
      .format("console")
      .queryName("query2")
      .start()

    val query3: StreamingQuery = result.writeStream
      .format("console")
      .queryName("query3")
      .start()

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
        println("Query started: " + queryStarted.id)
      }

      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
        println("Query terminated: " + queryTerminated.id)
      }

      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {

        if ("query1".equals(queryProgress.progress.name)) {
          println("query1 watermark : " + queryProgress.progress.eventTime.get("watermark"))
        } else if ("query2".equals(queryProgress.progress.name)) {
          println("query2 watermark : " + queryProgress.progress.eventTime.get("watermark"))
        } else {
          println("query3 watermark : " + queryProgress.progress.eventTime.get("watermark"))
        }
      }
    })

    spark.streams.awaitAnyTermination()


  }
}
