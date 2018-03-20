package com.quchengkai.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/22.
  */
object VisualCinema_Cluster {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Movie_Analyzer"))
    val cinemaRDD = sc.textFile(args(0) + "Cinema.txt")
    val cinemaBoRDD = sc.textFile(args(0) + "Cinema_box_office.txt")

    //电影院总人数
    val cinemaDataTwo = cinemaBoRDD.map(_.split(",")).map(x => (x(2), x(5))).cache()
    cinemaDataTwo.map(x => (x._1, x._2.toLong))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2.toLong))
      .reduceByKey(_ + _)
      .map(x => (x._1, x._2.toLong))

    //可视化人数最多的影院所需要的数据
    val cinemaDataOne = cinemaRDD.map(_.split(","))
      .map(x => (x(1), x(2), x(3), x(4), x(5), x(6), x(7)))
      .map(x => (x._1, (x._2, x._3, x._4, x._5, x._6, x._7)))

    val visualizationData = cinemaDataTwo.join(cinemaDataOne).cache()
    visualizationData.map(x => ((x._1, x._2._2._1, x._2._2._2, x._2._2._3,
      x._2._2._4, x._2._2._5, x._2._2._6), x._2._1.toLong))
      .reduceByKey(_ + _)
      .map(x => (x._2.toLong, x._1))
      .sortBy(_._1, false)
      .saveAsTextFile(args(1))

    sc.stop()
  }
}