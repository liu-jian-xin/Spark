package com.arguigu.spark.core.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("filter").setMaster("local[5]")

    val sc = new SparkContext(conf)

    val list1 = List(1,4,38,8,9,10,0)

    val rdd1: RDD[Int] = sc.parallelize(list1)
//    println(rdd1.getNumPartitions)
//    val rdd2: RDD[Int] = rdd1.filter(_>5)

//    val rdd2: RDD[Int] = rdd1.sample(false,0.1)
//    val rdd2: RDD[Int] = rdd1.coalesce(2)
//    val rdd2: RDD[Int] = rdd1.repartition(7)
//       val rdd2: RDD[Int] = rdd1.sortBy(x => x)
//    val rdd3: RDD[Int] = rdd1.sortBy(x=>x,false)
//    println(rdd2.getNumPartitions)
//     rdd2.collect().foreach(println)
//   val rdd2 = rdd1.collect{
//      case i:Int if i>30 => i+10
//    }
    val rdd2= rdd1.map(x=> if(x> 30) Array(x+10) else Array[Int]())
    rdd2.collect().foreach(x => println(x.mkString(",")))
//val rdd2 = rdd1.map(x => if(x >= 50) Array(x) else Array[Int]())
//    rdd2.collect.foreach(x => println(x.mkString(",")))
  }
}
