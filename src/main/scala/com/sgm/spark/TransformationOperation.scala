package com.sgm.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TransformationOperation {

  def main(args: Array[String]) {
    // map()
    // filter()
    // flatMap()
    // groupByKey()
    // reduceByKey()
    // sortByKey()
    join()
  }

  def map() {
    val conf = new SparkConf()
      .setAppName("map")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)
    val numberRDD = sc.parallelize(numbers, 1)
    val multipleNumberRDD = numberRDD.map { num => num * 2 }

    multipleNumberRDD.foreach { num => println(num) }
  }

  def filter() {
    val conf = new SparkConf()
      .setAppName("filter")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers, 1)
    val evenNumberRDD = numberRDD.filter { num => num % 2 == 0 }

    evenNumberRDD.foreach { num => println(num) }
  }

  def flatMap() {
    val conf = new SparkConf()
      .setAppName("flatMap")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lineArray = Array("hello you", "hello me", "hello world")
    val lines = sc.parallelize(lineArray, 1)
    val words = lines.flatMap { line => line.split(" ") }

    words.foreach { word => println(word) }
  }

  def groupByKey() {
    val conf = new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),
      Tuple2("class1", 90), Tuple2("class2", 60))
    val scores = sc.parallelize(scoreList, 1)
    val groupedScores = scores.groupByKey()

    groupedScores.foreach(score => {
      println(score._1)
      score._2.foreach { singleScore => println(singleScore) }
      println("=============================")
    })
  }

  def reduceByKey() {
    val conf = new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),
      Tuple2("class1", 90), Tuple2("class2", 60))
    val scores = sc.parallelize(scoreList, 1)
    val totalScores = scores.reduceByKey(_ + _)

    totalScores.foreach(classScore => println(classScore._1 + ": " + classScore._2))
  }

  def sortByKey() {
    val conf = new SparkConf()
      .setAppName("sortByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val scoreList = Array(Tuple2(65, "leo"), Tuple2(50, "tom"),
      Tuple2(100, "marry"), Tuple2(85, "jack"))
    val scores = sc.parallelize(scoreList, 1)
    val sortedScores = scores.sortByKey(false)

    sortedScores.foreach(studentScore => println(studentScore._1 + ": " + studentScore._2))
  }

  def join() {
    val conf = new SparkConf()
      .setAppName("join")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val studentList = Array(
      Tuple2(1, "leo"),
      Tuple2(2, "jack"),
      Tuple2(3, "tom"),
      Tuple2(2, "joe"))
    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(2, 80),
      Tuple2(4, 60))
    val students = sc.parallelize(studentList)
    val scores = sc.parallelize(scoreList)

    val studentScores = students.cogroup(scores)
    studentScores.collect.foreach(println)
    /*输出ID相同的输出多行,不全的直接剔除
    (1,(leo,100))
    (2,(jack,90))
    (2,(jack,80))
    (2,(joe,90))
    (2,(joe,80))*/
    studentScores.foreach(studentScore => {
      println("student id: " + studentScore._1)
      println("student name: " + studentScore._2._1)
      println("student socre: " + studentScore._2._2)
      println("=======================================")
    })
  }

  def cogroup() {
    /*join直接改为cogroup,与join不同:输出格式不同,相同ID一起,不全的为空
    (4,(CompactBuffer(),CompactBuffer(60)))
    (1,(CompactBuffer(leo),CompactBuffer(100)))
    (3,(CompactBuffer(tom),CompactBuffer()))
    (2,(CompactBuffer(jack, joe),CompactBuffer(90,80)))
     */
  }

}