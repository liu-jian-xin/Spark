package com.arguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.textFile("E:\\1.txt")

    val wordAndCount: RDD[(String, Int)] = rdd1.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    val result: Array[(String, Int)] = wordAndCount.collect()

    result.foreach(println)

    sc.stop()

  }
}
