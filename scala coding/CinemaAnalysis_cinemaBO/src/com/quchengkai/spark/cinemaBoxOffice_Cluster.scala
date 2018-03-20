package com.quchengkai.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/22.
  */
object cinemaBoxOffice_Cluster {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Movie_Analyzer"))
    val cinemaBoRDD = sc.textFile(args(0) + "Cinema_box_office.txt")

    //电影院总票房
    val cinemaData = cinemaBoRDD.map(_.split(",")).map(x => (x(2), x(4)))
      .map(x => (x._1, x._2.toDouble))
      .reduceByKey(_ + _)
      .map(x => (x._2.toDouble, x._1))
      .sortByKey(false)
      .saveAsTextFile(args(1))

    sc.stop()
  }
}