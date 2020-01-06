package com.arguigu.spark.core.day02

import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf对象, 并设置 App名字
    val conf: SparkConf = new SparkConf().setAppName("WordCount")
    // 2. 创建SparkContext对象
    val sc = new SparkContext(conf)
    // 3. 使用sc创建RDD并执行相应的transformation和action
    sc.textFile("hdfs://hadoop102:9000/input/hello")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile("hdfs://hadoop102:9000/output")
    // 4. 关闭连接
    sc.stop()
  }


}
