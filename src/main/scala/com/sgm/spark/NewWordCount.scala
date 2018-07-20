package com.sgm.spark
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
object NewWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }
    // Create the context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(20))
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.flatMap(_.split(" "))
    //val word=lines.flatMap(x=>x.split(" ")) 是words的复杂写法
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _).map(line=>(line._2,line._1))
    wordCounts.print()
    wordCounts.foreachRDD(line=>line.sortByKey({false}).take(3).foreach(println))
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println
