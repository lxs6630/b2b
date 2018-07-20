package com.sgm.spark
import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._

/**
  * 数据清洗放入mysql
  */
object NginxClear {
  def main(args: Array[String]) {
    if (args.length !=2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <bootstrap.servers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    // Get the lines, split them into words, count the words and print
    //    val lines = messages.map(_.value)
    //    val words = lines.flatMap(_.split(" "))
    //    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    //    wordCounts.print()
    //数据清洗出访问时间，ip,状态，url
    val logs=messages.map(_.value)
    val cleanDate=logs.map(line=>{
      val infos=line.split(" ")
      val actime=infos(3).split(" ")(0).split("\\[")(1)
      ClearLog(DateUtil.parseTime(actime),infos(0),infos(8).toInt,infos(10))
    })//.filter(clearlog=>clearlog.statusCode != 200)
    //统计访问量，状态
    val statusCount=cleanDate.map(x=>{
      (x.statusCode,1)
      //(x.time.substring(0, 12), 1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        if(partitionRecords.nonEmpty) {
          val connection= createConnettion()
          partitionRecords.foreach(pair => {
            val sql="insert into statuscount(stauts,statcount) values("+pair._1+","+pair._2+")"
            connection.createStatement().execute(sql)
          })
          connection.close()
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取mysql数据库连接
    * @return
    */
  def createConnettion()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://192.168.10.128:3306/t1","root","123456")
  }
}

