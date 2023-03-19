package com.spark.structured

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import java.util.concurrent.TimeUnit

object readSocket {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .appName("read_socket")
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

    import session.implicits._
    val frame: DataFrame = df.as[String].flatMap(_.split(" ")).groupBy("value").count()

    // 打印数据
    val query: StreamingQuery = frame.writeStream
      // 打印到 console 界面
      .format("console")
      // 流式数据是 批处理还是连续实时数据
      // Unspecified(默认)： Trigger.ProcessingTime(0L) 。 没有规定触发类型则按照默认微批模式执行
      // 固定间隔批次：Trigger.ProcessingTime(5, TimeUnit.SECONDS) 设置微批次处理时间间隔 5秒
      // 一次性触发：Trigger.Once() 只执行一个微批次处理所有可用数据，之后自动停止。
      .trigger(Trigger.ProcessingTime(0L))
      // 指定输出模式 默认为 append(追加模式) ，不支持聚合查询输出结果， 支持：select、where、map、flatmap、filter、join
      // complete(完整模式):仅可用于代码中有聚合情况的时候，无聚合不能使用。
      // update(更新模式): 只有自上次触发后更新过的行才会被打印。
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}
