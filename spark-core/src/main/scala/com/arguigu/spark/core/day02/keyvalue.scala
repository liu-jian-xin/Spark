package com.arguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object keyvalue {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("kv").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4,5))
    val rdd2: RDD[Int] = sc.parallelize(List(4,5,6,7,8,2))
//    val rdd3: RDD[Int] = rdd1.union(rdd2)
//    val rdd3: RDD[Int] = rdd1.subtract(rdd2)
//    val rdd3: RDD[Int] = rdd1.intersection(rdd2)
//    val rdd3: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
//    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
//    val rdd3: RDD[(Int, Long)] = rdd1.zipWithIndex()
    val rdd3= rdd1.zipPartitions(rdd2)((it1,it2)=> it1.zipAll(it2,-1,-2))

    rdd3.foreach(println)


  }
}
