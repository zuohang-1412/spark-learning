package com.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object topNScala {
  def main(args: Array[String]): Unit = {
    // 综合应用算子
    val conf: SparkConf = new SparkConf().setAppName("topN_scala").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // 2019-6-1	39
    val fileRDD: RDD[String] = sc.textFile("data/topN.txt", 4)
    // 2019 6 1 39
    val dataRDD: RDD[(Int, Int, Int, Int)] = fileRDD.map(line => {
      line.split("\t")
    }).map(arr => {
      val dateStr: Array[String] = arr(0).split('-')
      (dateStr(0).toInt, dateStr(1).toInt, dateStr(2).toInt, arr(1).toInt)
    })
    // 分组取topN
    // 隐式转换 resSort 会覆盖原来的 增序排序
    implicit val resSort = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compareTo(x._2)
    }

    // 第一种 两个问题 ：1。groupByKey 容易oom 2。创建HashMap 存储数据，容易出现 oom
    println("----第一种----")
    val groupRDD1: RDD[((Int, Int), Iterable[(Int, Int)])] = dataRDD.map(t4 => {
      ((t4._1, t4._2), (t4._3, t4._4))
    }).groupByKey()
    val resRDD: RDD[((Int, Int), List[(Int, Int)])] = groupRDD1.mapValues(arr => {
      val hm = new mutable.HashMap[Int, Int]()
      arr.foreach(
        e => {
          if (hm.get(e._1).getOrElse(0) < e._2) hm.put(e._1, e._2)
        }
      )
      hm.toList.sorted.take(2)
    })
    resRDD.foreach(println)

    // 第二种 1.利用reduceByKey 进行去重，利用shuffle 去换空间
    println("----第二种----")
    val reduceRDD2: RDD[((Int, Int, Int), Int)] = dataRDD.map(t4 => {
      ((t4._1, t4._2, t4._3), t4._4)
    }).reduceByKey((x: Int, y: Int) => {
      if (y > x) y else x
    })
    val newRDD2: RDD[((Int, Int), (Int, Int))] = reduceRDD2.map(t2 => {
      ((t2._1._1, t2._1._2), (t2._1._3, t2._2))
    })
    val groupRDD2: RDD[((Int, Int), Iterable[(Int, Int)])] = newRDD2.groupByKey()
    val resRDD2: RDD[((Int, Int), List[(Int, Int)])] = groupRDD2.mapValues(e => e.toList.sorted.take(2))
    resRDD2.foreach(println)

    // 第三种
    println("----第三种----")
    //  用了groupByKey  容易OOM  取巧：用了spark 的RDD  sortByKey 排序  没有破坏多级shuffle的key的子集关系
    val sortedRDD: RDD[(Int, Int, Int, Int)] = dataRDD.sortBy(t4 => (t4._1, t4._2, t4._4), false)
    val groupByRDD: RDD[((Int, Int), Iterable[(Int, Int)])] = sortedRDD.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
    val newMapRDD: RDD[((Int, Int), Array[(Int, Int)])] = groupByRDD.mapValues(f => {
      val temp = Array((0, 0), (0, 0), (0, 0))
      val vl: List[(Int, Int)] = f.toList
      for (i <- 0 until vl.length) {
        var flg = 0
        for (j <- 0 until temp.length) {
          if (temp(j)._1 == vl(i)._1) {
            if (temp(j)._2 < vl(i)._2) {
              flg = 1
              temp(j) = vl(i)
            } else {
              flg = 2
            }
          }
        }
        if (flg == 0) {
          temp(temp.length - 1) = vl(i)
        }
        scala.util.Sorting.quickSort(temp)
      }
      temp.sorted.take(2)
    })
    newMapRDD.map(e=>(e._1,e._2.toList)).foreach(println)


    // 第四种
    println("----第四种----")
    val valueRDD3: RDD[((Int, Int), (Int, Int))] = dataRDD.map(t4 => {
      ((t4._1, t4._2), (t4._3, t4._4))
    })

    val cbkRDD: RDD[((Int, Int), Array[(Int, Int)])] = valueRDD3.combineByKey(
      // 放置第一条记录
      (v1: (Int, Int)) => {
        // 创建长度为3 的数组 第三个是临时位
        Array(v1, (0, 0), (0, 0))
      },
      // 放置后n条数据
      (oldV: Array[(Int, Int)], newV: (Int, Int)) => {
        var flg = 0
        for (i <- 0 until oldV.length) {
          // 日期相同比温度大小
          if (oldV(i)._1 == newV._1) {
            if (oldV(i)._2 < newV._2) {
              flg = 1
              oldV(i) = newV
            } else {
              flg = 2
            }
          }
        }
        if (flg == 0) {
          oldV(oldV.length - 1) = newV
        }
        scala.util.Sorting.quickSort(oldV)
        oldV
      },
      // 合并排序
      (v1: Array[(Int, Int)], v2: Array[(Int, Int)]) => {
        val temp = Array((0, 0), (0, 0), (0, 0))
        val v3: Array[(Int, Int)] = v1.union(v2)
        for (i <- 0 until v3.length) {
          var flg = 0
          for (j <- 0 until temp.length) {
            if (temp(j)._1 == v3(i)._1) {
              if (temp(j)._2 < v3(i)._2) {
                flg = 1
                temp(j) = v3(i)
              } else {
                flg = 2
              }
            }
          }
          if (flg == 0) {
            temp(temp.length - 1) = v3(i)
          }
          scala.util.Sorting.quickSort(temp)
        }
        temp.sorted.take(2)
      }
    )
    cbkRDD.map(e => (e._1, e._2.toList)).foreach(println)

  }
}
