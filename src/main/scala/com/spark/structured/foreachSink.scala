package com.spark.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement}

// foreachSink foreachBatch, 一批次进行保存， 自定义保存数据
object foreachSink {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("foreach_sink")
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
    val userDF: DataFrame = df.as[String].map(
      e => {
        val arr: Array[String] = e.split(",")
        (arr(0).toInt, arr(1), arr(2).toInt)
      }
    ).toDF("id", "name", "age")

    val query: StreamingQuery = userDF.writeStream.foreach(new ForeachWriter[Row] {
      var conn: Connection = _
      var pst: PreparedStatement = _

      override def open(partitionId: Long, epochId: Long): Boolean = {
        // 创建资源
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/data", "root", "zuohang123")
        pst = conn.prepareStatement("insert into foreachSink values (?,?,?)".stripMargin)
        true
      }

      override def process(value: Row): Unit = {
        // 处理数据
        val id: Int = value.getInt(0)
        val name: String = value.getString(1)
        val age: Int = value.getInt(2)
        pst.setInt(1, id)
        pst.setString(2, name)
        pst.setInt(3, age)
        pst.executeUpdate()
      }

      override def close(errorOrNull: Throwable): Unit = {
        // 关闭释放资源
        conn.close()
        pst.close()
      }
    }).start()


    query.awaitTermination()
  }
}
