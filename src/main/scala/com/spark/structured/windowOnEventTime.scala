package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.text.SimpleDateFormat

/*
 1641780000000 zhangSan,liSi,wangWu,zhangSan
 1641780002000 zhangSan,liSi,wangWu
 1641780005000 lisi,maLiu,liSi
 1641780010000 zhangSan,liSi
 1641780003000 wangWu,zhangSan
*/

// 窗口划分规则：所有窗口的划分都是从 1970-01-01 00:00:00 UTC 时间开始
// 具体逻辑参照： org.apache.spark.sql.catalyst.analysis.TimeWindowing
// waterMark 处理延迟数据
object windowOnEventTime {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("window_on_event_time")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
    import session.implicits._

    val df: DataFrame = session
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // 数据格式：1641780000000 zhangSan,liSi,wangWu,zhangSan
    val wordDF: DataFrame = df.as[String].flatMap(line => {
      val ts: String = line.split(" ")(0)
      val tuples: Array[(Timestamp, String)] = line
        .split(" ")(1)
        .split(",")
        .map((new Timestamp(ts.toLong), _))
      tuples
    }).toDF("ts", "word")

    import org.apache.spark.sql.functions._
    // 设置窗口 窗口长度是10秒 滑动间隔是5秒i哦                                                                                                  ws
    // waterMark 必须设置在group 之前

    val groupDF: DataFrame = wordDF
      // 哪个字段设置过期时间 必须在groupBy 之前
      .withWatermark("ts", "5 seconds")
      .groupBy(window($"ts", "10 seconds", "5 seconds"), $"word")
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
      // orderBy 只能在complete 中使用
//      .orderBy("start_time", "end_time")
      .writeStream.format("console")
      // outputMode 对于 waterMark 会有不同
      // complete 模式： 会打印所有数据，维护所有数据状态，和waterMark 没有关系，会输出全部数据。
//      .outputMode("complete")
      // update 模式：如果waterMark 值没有超过时间窗口的endTime之间，有迟到的数据仍然会被触发记录在该窗口。
      .outputMode("update")
      // append 模式：watermark 大于等于一个窗口的结束时间，那么这个窗口的数据才会输出（主推）
      .outputMode("append")
      .start()
    query.awaitTermination()
  }
}
