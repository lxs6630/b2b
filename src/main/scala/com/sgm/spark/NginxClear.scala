package com.sgm.spark
import scala.util
import scalaj.http._
import org.apache.spark.SparkConf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
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

    // Create context with 30 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    ssc.checkpoint("C:\\Users\\itlocal\\IdeaProjects\\nginxlog\\checkpoint")
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)  //采用checkpoint提交记录的偏移量，没有的话执行auto.offset.reset
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val logs=messages.map(_.value)
    val clearDate=logs.map(line=> {
      val info = line.split("\n")
      val infoPattern ="""(\d+\.\d+\.\d+\.\d+)(.*)(\d+/[A-Z,a-z]+/\d+\:\d+\:\d+\:\d+)(.*)([1-5]{1}[0-9]{2}) (\d+) (\"http.*\") (\".*\") (.*)""".r
      info.foreach(x=>x match {
        case infoPattern(ip,none1,currentTime,none2,respCode,requestTime,url,none3,upstreamTIME)=>
          println(ip+"\t"+currentTime+"\t"+respCode+"\t"+requestTime+"\t"+url+"\t"+upstreamTIME+"\t")
        case _=>println("none")
      })
    })
    clearDate.count().print()
    /*val cleanDate=logs.map(line => {
      val influxdb_url = "http://192.168.10.129:8086/write?db=nginxlog_clear"
      val infos = line.split(" ")
      if (infos.length>10) {
        val actime = infos(3).split(" ")(0).split("\\[")(1)
        val random_num = (new util.Random).nextInt(999999)
        val curent_timestamp = (DateUtil.getTime(actime).toString + "000000").toLong + random_num  //influxdb精确到纳秒,而时间戳到毫秒，不转换成纳秒时间戳不识别
        val urlPattern="\"http.*".r
        val respcodePattern="^[1-5]{1}[0-9]{2}$".r
        infos.foreach(println)
        var initUrl="\"none\""
        var initStatus="\"none\""
        infos.foreach(info=>urlPattern.findAllIn(info).foreach(x=>initUrl=Some(x).get))
        infos.foreach(info=>respcodePattern.findAllIn(info).foreach(x=>initStatus=x))
        println(initStatus)
        val date = s"nginxLog,ip=${infos(0)},acess_time=${DateUtil.parseTime(actime)},status=$initStatus,upstream_time=${infos.last} send_bytes=${infos(9)},url=$initUrl,count=1 $curent_timestamp"
        println(date)
        Http(influxdb_url).postData(date).header("content-type", "application/json").asString.code
      }
    })//.filter(clearlog=>clearlog.statusCode != 200)
    cleanDate.count().print()*/
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}

