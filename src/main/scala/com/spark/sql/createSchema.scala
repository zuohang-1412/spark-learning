package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SparkSession}

import scala.beans.BeanProperty

class Person extends Serializable {
  @BeanProperty
  var name: String = ""
  @BeanProperty
  var age: Int = 0
}

object createSchema {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("df_test").master("local").getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")
    val rdd: RDD[String] = sc.textFile("data/person.txt")

    // 第一种 rdd[row] + StructType
    println("---- 第一种 ----")
    // 数据 RDD[Row]
    val rddRow: RDD[Row] = rdd.map(line => {
      // zhangsan 18
      val strs: Array[String] = line.split(" ")
      Row.apply(strs(0), strs(1).toInt)
    })

    // 元数据
    val name: StructField = StructField.apply("name", DataTypes.StringType, true)
    val age: StructField = StructField.apply("age", DataTypes.IntegerType, true)
    val schema: StructType = StructType.apply(Array(name, age))
    val df: DataFrame = session.createDataFrame(rddRow, schema)
    df.show()
    df.printSchema()
    df.createTempView("event")
    session.sql("select distinct name, age from event").show()

    // 第二种 半动态 rdd[row] + StructType
    println("---- 第二种 ----")
    // Schema
    val userSchame: Array[String] = Array(
      "name:String:true",
      "age:Int:false"
    )

    // RDD[row]
    def getDataType(value: (String, Int)) = {
      userSchame(value._2).split(":")(1) match {
        case "String" => value._1.toString
        case "Int" => value._1.toInt
      }
    }


    val rddRow2: RDD[Row] = rdd.map(_.split(" "))
      .map(x => x.zipWithIndex)
      .map(x => x.map(getDataType(_)))
      .map(x => Row.fromSeq(x))

    // StructType
    def getStructType(value: String) = {
      value match {
        case "String" => DataTypes.StringType
        case "Int" => DataTypes.IntegerType
      }
    }

    val fields: Array[StructField] = userSchame.map(_.split(":")).map(x => StructField.apply(x(0), getStructType(x(1)), x(2).toBoolean))
    val schema2: StructType = StructType.apply(fields)
    val df2: DataFrame = session.createDataFrame(rddRow2, schema2)
    df2.show()
    df2.printSchema()

    // 第三种 bean类型的rdd + javabean
    println("---- 第三种 ----")
    val rddPerson: RDD[Person] = rdd.map(_.split(" ")).map(arr => {
      val p = new Person
      p.setName(arr(0))
      p.setAge(arr(1).toInt)
      p
    })
    // fields 会根据字符串排序顺序进行创建
    val df3: DataFrame = session.createDataFrame(rddPerson, classOf[Person])
    df3.show()
    df3.printSchema()

    // 第四种 DataSet
    val rdd4: Dataset[String] = session.read.textFile("data/person.txt")
    val rddSet: Dataset[(String, Int)] = rdd4.map(line => {
      val strs: Array[String] = line.split(" ")
      (strs(0), strs(1).toInt)
    })(Encoders.tuple(Encoders.STRING, Encoders.scalaInt))
    val df4: DataFrame = rddSet.toDF("name", "age")
    df4.show()
    df4.printSchema()

  }
}
