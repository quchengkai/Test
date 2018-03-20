package com.quchengkai.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/6/22.
  */
object particularMovie_Cluster {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Movie_Analyzer"))
    val movieBoRDD = sc.textFile(args(0) + "Movie_box_office.txt")

    //特定电影的总票房
    val movieData = movieBoRDD.map(_.split(",")).map(x => (x(2), x(4), x(5)))
      .filter(_._2.equals("老炮儿"))
      .map(x => ((x._1,x._2), x._3.toLong))
      .reduceByKey(_ + _)
      .map(x => (x._2.toLong, x._1))
      .sortByKey(false)
      .saveAsTextFile(args(1))

    sc.stop()
  }
}