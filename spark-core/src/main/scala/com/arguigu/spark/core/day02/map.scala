package com.arguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object map {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("map").setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val rdd1: RDD[Int] = sc.parallelize(1 to 10)
      val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    val rdd2: RDD[(Boolean, Iterable[Int])] = rdd1.groupBy(x => x%2==0)
//     val rdd2: RDD[Array[Int]] = rdd1.glom()
//    val rdd2: RDD[Int] = rdd1.mapPartitions(it => it.map(_*2))
//    val rdd2: RDD[Int] = rdd1.map(_*2)
//   val arr: Array[Int] = rdd2.collect()
//    val rdd2: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex((index,it) => it.map((_,index)) )
    ////      rdd2.collect().foreach(println)
//    val rdd2: RDD[Int] = rdd1.flatMap(x => Array(x*x,x*x*x))

    rdd2.collect().foreach(println)

    sc.stop()



  }

}
