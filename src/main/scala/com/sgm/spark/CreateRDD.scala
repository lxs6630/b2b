package com.sgm.spark
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
/*
使用集合创建RDD
 */
object CreateRDD {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("CreateRDD")
    val sc=new SparkContext(conf)
    val arrays=Array(1,2,3,4,5,6,7,8,9,10)
    val rddData=sc.parallelize(arrays,2) //使用集合创建RDD,序列化,默认根据集群情况设置rdd的partition，也可以手动传入
    val sum=rddData.reduce(_+_)
    //val rdd = sc.textFile("data.txt") //使用本地文件创建RDD
    //val wordCount = rdd.map(line => line.length).reduce(_ + _)  //map是transformation操作(transformation都是lazy执行),reduce是action,才执行操作
    //val lines = sc.textFile("hdfs://spark1:9000/spark.txt", 1) //使用hdfs文件创建RDD
    //val count = lines.map { line => line.length() }.reduce(_ + _)
    println(sum)
  }
}
