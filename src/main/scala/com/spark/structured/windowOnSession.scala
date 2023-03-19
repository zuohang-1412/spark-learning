package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.text.SimpleDateFormat


// 会话窗口 session_window
object windowOnSession {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("session_window")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    val df: DataFrame = session
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import session.implicits._
    val wordDF: DataFrame = df.as[String].flatMap(line => {
      val ts: String = line.split(" ")(0)
      val tuples: Array[(Timestamp, String)] = line
        .split(" ")(1)
        .split(",")
        .map((new Timestamp(ts.toLong), _))
      tuples
    }).toDF("ts", "word")

    import org.apache.spark.sql.functions._

    //todo window 改换成session_window 出现问题
    val groupDF: DataFrame = wordDF
      .groupBy(
        window($"ts", "10 seconds"),
        $"word")
      .count()

    groupDF.printSchema()

    val res: DataFrame = groupDF.map(row => {
      val start_time: Timestamp = row.getStruct(0).getTimestamp(0)
      val end_time: Timestamp = row.getStruct(0).getTimestamp(0)
      val word: String = row.getString(1)
      val count: Long = row.getLong(2)
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (sdf.format(start_time.getTime), sdf.format(end_time.getTime), word, count)
    }).toDF("start_time", "end_time", "word", "count")
    val query: StreamingQuery = res
      .writeStream.format("console")
      .outputMode("complete")
      .start()
    query.awaitTermination()
  }
}
