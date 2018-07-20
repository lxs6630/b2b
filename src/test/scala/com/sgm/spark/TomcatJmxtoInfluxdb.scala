//package com.sgm.spark
///*
//function: reading tomcat jmx  info from kafka cluster，process them, then put the result to influxdb.
//author: guanss
//date: 2018-03-21
//*/
//
////初始化Dbinfo类和DbinfoMap类
//import spray.json._
//case class Dbinfo(name:String,user:String,url:String)
//case class DbinfoMap(key:String,dbinfo:List[Dbinfo])
//object MyJsonProtocol extends DefaultJsonProtocol {
//  implicit val dbinfosonFormat = jsonFormat3(Dbinfo)
//  implicit val dbinfoFormat = jsonFormat2(DbinfoMap)
//}
//import MyJsonProtocol._
//
//import collection.JavaConversions._ //非常重要，scala2.8之后提供的java和scala对象的隐式转换
//import java.io.Serializable
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.serializer.Decoder
//import kafka.utils.ZKStringSerializer
//import org.apache.spark.streaming.kafka.KafkaCluster.{LeaderOffset}
//import org.I0Itec.zkclient.ZkClient
//import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
//import org.apache.spark.SparkException
//import org.apache.zookeeper.data.Stat
//import org.slf4j.LoggerFactory
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka._
//import org.apache.hadoop.util.ClassUtil;
//import org.apache.spark.streaming.dstream.DStream;
//import scala.reflect.ClassTag
//import java.sql.Timestamp
//import java.util.Locale
//import java.text.SimpleDateFormat
//import java.util.Date
//import scalaj.http._
//import org.apache.log4j.{Level, Logger}
//import com.alibaba.fastjson.JSON
//import redis.clients.jedis._
//import java.lang.management.MemoryUsage
//import javax.management.MBeanAttributeInfo
//import javax.management.MBeanInfo
//import javax.management.MBeanServerConnection
//import javax.management.ObjectInstance
//import javax.management.ObjectName
//import javax.management.openmbean.CompositeDataSupport
//import javax.management.remote.JMXConnector
//import javax.management.remote.JMXConnectorFactory
//import javax.management.remote.JMXServiceURL
//import scala.util.control._
//import util._
//import concurrent.ExecutionContext.Implicits.global
//import concurrent.Future
//import concurrent.duration.{Duration=>MyDuration,SECONDS}
//import java.util.concurrent.ConcurrentHashMap   //
//
//class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {
//  //定义一个kafka集群对象
//  private val kc = new KafkaCluster(kafkaParams)
//
//  //定义创建直连kafka流的函数
//  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K]: ClassTag, VD <: Decoder[V]: ClassTag](ssc: StreamingContext, kafkaParams: Map[String, String], topics: Set[String]): InputDStream[(K, V)] =  {
//
//    //从map(映射)中读取group.id
//    val groupId = kafkaParams.get("group.id").get
//
//    //从kc中获取partitions partions 的值是一个set，Set([nginx_log_test,0], [nginx_log_test,1]）
//    val partitions = kc.getPartitions(topics).right.get
//
//    //从kc中获取consumerOffsets  consumerOffsets的值是一个map，Map([nginx_log_test,0] -> 10338, [nginx_log_test,1] -> 10339)
//    val consumerOffsets_tmp = kc.getConsumerOffsets(groupId, partitions)
//
//    //判断消费者offset记录是否存在，如果不存在，则不传入offsets，如果存在，则传入offsets  *如果已有offset，但是想从最新开始，删掉zk上的offsets记录即可*
//    if (consumerOffsets_tmp.isLeft) {
//      //调用KafkaUtils类的createDirectStream方法，创建直连kafka流，不传入offsets
//      val messages = KafkaUtils.createDirectStream[K, V, KD, VD](ssc, kafkaParams, topics)
//      return messages
//    }else{
//      val consumerOffsets = consumerOffsets_tmp.right.get
//      //调用KafkaUtils类的createDirectStream方法，创建直连kafka流，传入offsets
//      val messages = KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
//      return messages
//    }
//  }
//
//  //定义函数，用于更新update的offsets，当事物处理完毕时，将rdd中的offsetRanges取出，插入到zk中
//  def update_offsets(rdd: RDD[(String, String)]) : Unit = {
//
//    //从map(映射)中读取group.id
//    val groupId = kafkaParams.get("group.id").get
//
//    //从rdd对象中获取offsetRanges
//    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//    //遍历offsetRanges
//    for (offsets <- offsetsList) {
//
//      //用TopicAndPartition方法初始化 topic和partition
//      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
//
//      //调用kc的setConsumerOffsets方法，更新offsets
//      kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)))
//    }
//  }
//}
//
//object TomcatJmxtoInfluxdb {
//
//  Logger.getLogger("org").setLevel(Level.ERROR)
//
//  //定义函数，用于将jmx探测结果中的异常情况进行告警
//  def alert(result:Map[String,Any]):List[Map[String,String]]={
//    var alert_list = List[Map[String,String]]()
//    val ip = result.get("ip").getOrElse("")
//    val instance = result.get("instance").getOrElse("")
//    val team = result.get("team").getOrElse("")
//    val appname = result.get("appname").getOrElse("")
//    var error_value = ""
//    var level = ""
//    var error_code = ""
//    var timestamp = result.get("timestamp").getOrElse("")
//    val sdf = new SimpleDateFormat("yyyy-MM-dd H:mm:ss")
//    var data_collect_time = timestamp.asInstanceOf[String]
//    data_collect_time = data_collect_time.replaceAll("000000$",".000")
//    data_collect_time = sdf.format(data_collect_time.toDouble)
//    var alert_map = Map[String,String]("level"->level,"error_code"->error_code)
//    alert_map += ("ip"->ip.asInstanceOf[String])
//    alert_map += ("appname"->appname.asInstanceOf[String])
//    alert_map += ("team"->team.asInstanceOf[String])
//    alert_map += ("instance"->instance.asInstanceOf[String])
//    alert_map += ("data_collect_time"->data_collect_time)
//
//    //开始报警判断
//    if (result.get("scan_jmx_result").getOrElse(0) != 0){
//      //判断是否探测结果，如果探测失败，则添加报错记录
//      if (result.get("scan_jmx_result").getOrElse(0) == 1){
//        error_value = "jmx scan failed"
//        level = "high"
//        error_code = "200000"
//      }else{
//        error_value = "jmx scan timeout"
//        level = "warning"
//        error_code = "200001"
//      }
//      //添加报错内容到map，并添加到list中
//      alert_map += ("level"->level)
//      alert_map += ("error_code"->error_code)
//      alert_map += ("error_value"->error_value)
//      alert_list = alert_map :: alert_list;
//    }else{
//      //指标告警   堆内存使用率告警
//      val heap_memory_used = result.get("heap_memory_used").getOrElse("-2").asInstanceOf[String]
//      val heap_memory_max = result.get("heap_memory_max").getOrElse("-2").asInstanceOf[String]
//      if (heap_memory_used != "-2" && heap_memory_max != "-2" && heap_memory_max != "-1"){
//        val heap_memory_used_rate = heap_memory_used.toFloat*100/heap_memory_max.toFloat
//        if (heap_memory_used_rate >= 80){
//          level = "high"
//          error_code = "200002"
//          error_value = "heap_memory_used_rate over "+heap_memory_used_rate.toString
//          alert_map += ("level"->level)
//          alert_map += ("error_code"->error_code)
//          alert_map += ("error_value"->error_value)
//          alert_list = alert_map :: alert_list;
//        }
//      }
//
//      //指标告警
//    }
//    alert_list
//  }
//
//  //定义函数，用于将jmx探测结果进行入库操作
//  def put2influxdb(result:Map[String,Any]):String={
//    var message = ""
//    if (result.get("scan_jmx_result").getOrElse(3) == 0){
//      val ip = result.get("ip").getOrElse("")
//      val instance = result.get("instance").getOrElse("")
//      val team = result.get("team").getOrElse("")
//      val ThreadCount = result.get("ThreadCount").getOrElse("-2")
//      val activeSessions = result.get("activeSessions").getOrElse("-2")
//      val currentThreadCount = result.get("currentThreadCount").getOrElse("-2")
//      val noheap_memory_used = result.get("noheap_memory_used").getOrElse("-2")
//      var tomcat_version = result.get("tomcat_version").getOrElse("-2")
//      tomcat_version = tomcat_version.asInstanceOf[String].replaceAll(" ","").replaceAll("/","")  //剔除空格和特殊符号，否则influxdb无法作为tag
//      val heap_memory_used = result.get("heap_memory_used").getOrElse("-2")
//      val PeakThreadCount = result.get("PeakThreadCount").getOrElse("-2")
//      val maxActiveSessions = result.get("maxActiveSessions").getOrElse("-2")
//      val heap_memory_max = result.get("heap_memory_max").getOrElse("-2")
//      val noheap_memory_max = result.get("noheap_memory_max").getOrElse("-2")
//      val maxThreads = result.get("maxThreads").getOrElse("-2")
//      val appname = result.get("appname").getOrElse("")
//      val timestamp = result.get("timestamp").getOrElse("")
//      val empty = List[Map[String,String]]()
//      var db_source = result.get("db_source").getOrElse(empty)
//      message = s"tomcat_jvm,ip=$ip,team=$team,instance=$instance,tomcat_version=$tomcat_version,appname=$appname ThreadCount=$ThreadCount,PeakThreadCount=$PeakThreadCount,maxThreads=$maxThreads,currentThreadCount=$currentThreadCount,heap_memory_max=$heap_memory_max,heap_memory_used=$heap_memory_used,noheap_memory_used=$noheap_memory_used,noheap_memory_max=$noheap_memory_max,maxActiveSessions=$maxActiveSessions,activeSessions=$activeSessions $timestamp"
//      for (db <- db_source.asInstanceOf[List[Map[String,String]]]){
//        val db_type = db.get("dbtype").getOrElse("")
//        if (db_type == "c3p0"){
//          val minPoolSize = db.get("minPoolSize").getOrElse("-2")
//          var jdbcUrl = db.get("jdbcUrl").getOrElse("")
//          jdbcUrl = "\""+jdbcUrl+"\""
//          val maxPoolSize = db.get("maxPoolSize").getOrElse("-2")
//          var dataSourceName = db.get("dataSourceName").getOrElse("default")
//          val maxIdleTime = db.get("maxIdleTime").getOrElse("-2")
//          val numConnections = db.get("numConnections").getOrElse("-2")
//          val numBusyConnections = db.get("numBusyConnections").getOrElse("-2")
//          val numIdleConnections = db.get("numIdleConnections").getOrElse("-2")
//          var user =  db.get("user").getOrElse("")
//          user =  "\""+user+"\""
//          val message1 = s"db_source,ip=$ip,team=$team,instance=$instance,tomcat_version=$tomcat_version,appname=$appname,db_type=c3p0,name=$dataSourceName minPoolSize=$minPoolSize,url=$jdbcUrl,maxPoolSize=$maxPoolSize,maxIdleTime=$maxIdleTime,numConnections=$numConnections,numBusyConnections=$numBusyConnections,numIdleConnections=$numIdleConnections,user=$user $timestamp"
//          message = message + "\n" + message1
//        }
//        if (db_type == "tomcatjdbc"){
//          val name = db.get("name").getOrElse("").replaceAll("\"","")
//          var url = db.get("Url").getOrElse("")
//          url = "\""+url+"\""
//          val NumActive = db.get("NumActive").getOrElse("-2")
//          val MaxIdle = db.get("MaxIdle").getOrElse("-2")
//          val MinIdle = db.get("MinIdle").getOrElse("-2")
//          var username =  db.get("Username").getOrElse("")
//          username =  "\""+username+"\""
//          val MaxActive = db.get("MaxActive").getOrElse("-2")
//          val message2 = s"db_source,ip=$ip,team=$team,instance=$instance,tomcat_version=$tomcat_version,appname=$appname,db_type=tomcatjdbc,name=$name url=$url,NumActive=$NumActive,MaxIdle=$MaxIdle,MinIdle=$MinIdle,user=$username,MaxActive=$MaxActive $timestamp"
//          message = message + "\n" + message2
//        }
//      }
//    }
//    message
//  }
//
//  //定义main入口
//  def main(args: Array[String]) {
//    //定义参数
//    val brokers = "172.21.197.193:9092,172.21.197.194:9092,172.21.197.195:9092,172.21.197.196:9092" //定义kafka brokers  注意是字符串
//    val topics = "tomcat-mes" //定义topic
//    val groupid = "Bdat" //定义kafka消费者的groupid  Big data analysis tool
//    val appname = "tomcat_jmx_to_influxdb"  //定义任务名称
//    val master = "yarn-cluster"  //定义master，注意cdh用了yarn框架，值应该是"yarn-client"
//    val influxdb_url = "http://172.21.197.200:8086/write?db=tomcat"  //定义influxdb地址
//    val batchduration : Int = 10  //定义批次间隔时间
//    val redis_ip = "172.21.197.201" //告警信息redis host
//    val redis_port = 6379 // 告警信息redis port
//    val monitor_queue = "monitor_queue" //告警信息插入queue
//    val dbinfo_key = "dbinfo4spark" //数据源信息的queue
//
//    //set 集合数据结构
//    val topicsSet = topics.split(",").toSet
//
//    //Map 映射，在scala语言中类似于python的字典，也是一种kv结构
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
//      "group.id" -> groupid,
//      "auto.offset.reset" -> "largest")
//    //创建一个kafka管理类对象
//    val km = new KafkaManager(kafkaParams)
//
//    //创建spark streaming context实例
//    val sparkConf = new SparkConf().setAppName(appname).setMaster(master)
//    val ssc =  new StreamingContext(sparkConf, Seconds(batchduration))
//
//    //使用reduce等高级算子时，必须使用checkpoint
//    ssc.checkpoint("checkpoint_tomcat_test")
//
//    //初始化dbinfomap
//    var dbinfomap = new ConcurrentHashMap[String,List[Dbinfo]]()
//
//    //定义函数，用于从外部redis获取dbinfo信息，并更新到dbinfomap中
//    val update_dbinfomap_from_redis = ()=>{
//      val jredis_client = new Jedis(redis_ip,redis_port)
//      val dbinfo_string = jredis_client.get(dbinfo_key)
//      jredis_client.disconnect()
//      val dbinfo_list:List[DbinfoMap] = dbinfo_string.parseJson.convertTo(DefaultJsonProtocol.listFormat[DbinfoMap])
//      for (dbinfo <- dbinfo_list){
//        dbinfomap.put(dbinfo.key,dbinfo.dbinfo)
//      }
//    }
//
//    //第一次更新dbinfomap
//    update_dbinfomap_from_redis()
//
//    //定义子线程，while循环，每隔1h获取更新一次dbinfomap
//    class UpdateMap() extends Thread{
//      override def run(){
//        while(true){
//          Thread.sleep(3600000)
//          try{
//            update_dbinfomap_from_redis()
//          }catch{
//            case _ : Throwable =>;
//          }
//        }
//      }
//    }
//
//    // 启动子线程
//    var t = new UpdateMap()
//    t.start()
//
//    //调用km对象的createDirectStream方法,创建dstream
//    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet).cache()
//
//    //定义函数，用于将用jmx探测tomcat配置信息
//    val scan_jmxson = (result1:Map[String,Any])=>{
//      var result = result1
//      var db_source = List[Map[String,String]]()
//      var port = ""
//      val ip = result.get("ip").getOrElse("")
//      val jmx = result.get("jmx").getOrElse("")
//      val http = result.get("http").getOrElse("")
//      val instance = result.get("instance").getOrElse("")
//      val map = Map("jmx.remote.credentials"->Array("controlRole", "R&D"))
//      val serviceURL = new JMXServiceURL(s"service:jmx:rmi:///jndi/rmi://$ip:$jmx/jmxrmi")
//      try{
//        //connect jmx
//        val connector = JMXConnectorFactory.connect(serviceURL,map)
//        val mbsc = connector.getMBeanServerConnection()
//
//        //tomcat version
//        val tomcat_version_objname = new ObjectName("Catalina:type=Server")
//        result += ("tomcat_version"->mbsc.getAttribute(tomcat_version_objname, "serverInfo").toString)
//
//        //maxThreads  currentThreadCount
//        val threadpool = new ObjectName("Catalina:type=ThreadPool,*")
//        val set1 = mbsc.queryNames(threadpool, null)
//        val loop1 = new Breaks;
//        loop1.breakable{
//          for (obj <- set1){
//            val objname = new ObjectName(obj.getCanonicalName())
//            try{
//              port = mbsc.getAttribute(objname, "port").toString;
//            }catch{
//              case _ : Throwable =>;
//            }
//            if ( port == http ){
//              result += ("maxThreads"->mbsc.getAttribute(objname, "maxThreads").toString)
//              result += ("currentThreadCount"->mbsc.getAttribute(objname, "currentThreadCount").toString)
//              loop1.break;
//            }
//          }
//        }
//
//        //heap_memory_used heap_memory_max noheap_memory_used noheap_memory_max
//        val heapObjName = new ObjectName("java.lang:type=Memory");
//        val HeapMemoryUsage_tmp = mbsc.getAttribute(heapObjName,"HeapMemoryUsage")
//        val heapMemoryUsage = MemoryUsage.from(HeapMemoryUsage_tmp.asInstanceOf[CompositeDataSupport]);
//        result += ("heap_memory_used"->heapMemoryUsage.getUsed().toString);
//        result += ("heap_memory_max"->heapMemoryUsage.getMax().toString);
//        val noHeapMemoryUsage_tmp = mbsc.getAttribute(heapObjName,"NonHeapMemoryUsage")
//        val noheapMemoryUsage = MemoryUsage.from(noHeapMemoryUsage_tmp.asInstanceOf[CompositeDataSupport]);
//        result += ("noheap_memory_used"->noheapMemoryUsage.getUsed().toString);
//        result += ("noheap_memory_max"->noheapMemoryUsage.getMax().toString);
//
//        //activeSessions maxActiveSessions
//        val managerObjName = new ObjectName("Catalina:type=Manager,*")
//        val set2 = mbsc.queryNames(managerObjName, null)
//        val loop2 = new Breaks;
//        loop2.breakable{
//          for (obj <- set2){
//            val objname = new ObjectName(obj.getCanonicalName())
//            var activeSessions = mbsc.getAttribute(objname,"activeSessions").toString
//            result += ("maxActiveSessions"->mbsc.getAttribute(objname, "maxActiveSessions").toString);
//            result += ("activeSessions"->activeSessions);
//            if (activeSessions != "0"){
//              loop2.break
//            }
//          }
//        }
//
//        //c3p0 dbsource
//        val c3p0ObjName = new ObjectName("com.mchange.v2.c3p0:type=PooledDataSource*");
//        val set3 = mbsc.queryNames(c3p0ObjName,null)
//        val dbinfo_tmp = dbinfomap.get(ip+"_"+instance)
//        for (obj <- set3){
//          val objname = new ObjectName(obj.getCanonicalName());
//          val jdbcUrl = mbsc.getAttribute(obj,"jdbcUrl").toString
//          val user = mbsc.getAttribute(obj,"user").toString
//          //遍历dbinfo_tmp，获取name
//          val loop3 = new Breaks;
//          var dataSourceName = "default"
//          loop3.breakable{
//            for(db <- dbinfo_tmp){
//              if (db.user == user && db.url == jdbcUrl){
//                dataSourceName = db.name
//                loop3.break;
//              }
//            }
//          }
//          val db = Map[String,String]("dbtype"->"c3p0",
//            "maxPoolSize"->mbsc.getAttribute(obj,"maxPoolSize").toString,
//            "minPoolSize"->mbsc.getAttribute(obj,"minPoolSize").toString,
//            "maxIdleTime"->mbsc.getAttribute(obj,"maxIdleTime").toString,
//            "numConnections"->mbsc.getAttribute(obj,"numConnections").toString,
//            "numBusyConnections"->mbsc.getAttribute(obj,"numBusyConnections").toString,
//            "numIdleConnections"->mbsc.getAttribute(obj,"numIdleConnections").toString,
//            "jdbcUrl"->jdbcUrl,
//            "user"->user,
//            "dataSourceName"->dataSourceName);
//          db_source = db :: db_source;
//        }
//
//        //tomcat.jdbc dbsource
//        val argument = s"tomcat.jdbc:type=ConnectionPool,engine=Catalina,host=localhost,*"
//        val jdbcObjName = new ObjectName(argument);
//        val set4 = mbsc.queryNames(jdbcObjName,null)
//        for (obj <- set4){
//          val objname = new ObjectName(obj.getCanonicalName());
//          val db = Map[String,String]("dbtype"->"tomcatjdbc",
//            "MaxActive"->mbsc.getAttribute(obj,"MaxActive").toString,
//            "Url"->mbsc.getAttribute(obj,"Url").toString,
//            "MinIdle"->mbsc.getAttribute(obj,"MinIdle").toString,
//            "MaxIdle"->mbsc.getAttribute(obj,"MaxIdle").toString,
//            "Username"->mbsc.getAttribute(obj,"Username").toString,
//            "NumActive"->mbsc.getAttribute(obj,"NumActive").toString,
//            "name"->objname.getKeyProperty("name"));
//          db_source = db :: db_source;
//        }
//        result += ("db_source"->db_source);
//
//        //ThreadCount  PeakThreadCount
//        val ThreadingObjName = new ObjectName("java.lang:type=Threading");
//        result += ("ThreadCount"->mbsc.getAttribute(ThreadingObjName, "ThreadCount").toString);
//        result += ("PeakThreadCount"->mbsc.getAttribute(ThreadingObjName,"PeakThreadCount").toString);
//
//        //close jmx connector
//        connector.close()
//        result += ("scan_jmx_result"->0); //0 表示成功
//      }catch{
//        case _ : Throwable => result += ("scan_jmx_result"->1); //1 表示失败
//      }
//      result
//    }
//
//    //定义函数，使用future进行超时判断
//    val scan_jmx = (line:String)=>{
//      var result = Map[String,Any]()
//      val json=JSON.parseObject(line)
//      val ip = json.get("ip").toString
//      val jmx = json.get("jmx").toString
//      val http = json.get("http").toString
//      val instance = json.get("instance").toString
//      val team = json.get("team").toString
//      val appname = json.get("appname").toString
//      result += ("ip"->ip)
//      result += ("jmx"->jmx)
//      result += ("http"->http)
//      result += ("instance"->instance)
//      result += ("team"->team)
//      result += ("appname"->appname)
//      result += ("instance"->instance)
//      //get timestamp
//      val now = new Date()
//      val timestamp = now.getTime + "000000"
//      result += ("timestamp"->timestamp)
//      try{
//        val result_tmp = concurrent.Await.result(Future(scan_jmxson(result)),MyDuration(3, SECONDS))
//        result = result ++ result_tmp
//      }catch{
//        case error:java.util.concurrent.TimeoutException =>{
//          result += ("scan_jmx_result"->2)  //2表示超时
//        }
//      }
//      result
//    }
//
//    //从kafka的读取的messages中读取envents,并缓存在内存中  map(_._2) 等价于  map(x=>x._2)
//    val lines = messages.map(_._2).map(scan_jmx).cache()
//
//    //入库操作
//    val process1 = lines.map(put2influxdb).reduce(_+"\n"+_).foreachRDD { (rdd: RDD[(String)]) =>
//      val data = rdd.collect().mkString(",").replaceAll("(\n\n+)","\n")
//      val res = Http(influxdb_url).postData(data).header("content-type", "application/json").asString.code
//    }
//
//    //用map reduce方法，将报错过滤检测失败的节点信息，插入redis
//    val process2 = lines.map(alert).reduce(_ ++: _).foreachRDD {(rdd) =>
//      var data = rdd.collect()
//      val jr = new Jedis(redis_ip, redis_port)
//      for(i <- data){
//        for(j <- i){
//          jr.rpush(monitor_queue,j.toJson.toString)
//        }
//      }
//      jr.disconnect()
//    }
//
//    //更新offsets  注意： rdd => {km.updateZKOffsets(rdd)}这种写法类似于python里面的lambda
//    messages.foreachRDD(rdd => {km.update_offsets(rdd)})
//
//    //启动和持续运行ssc
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
