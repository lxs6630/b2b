package com.sgm.spark
import collection.JavaConversions._ //非常重要，scala2.8之后提供的java和scala对象的隐式转换
import java.io.Serializable
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.serializer.Decoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkException
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.hadoop.util.ClassUtil;
import org.apache.spark.streaming.dstream.DStream;
import scala.reflect.ClassTag
import java.sql.Timestamp
import java.util.Locale
import java.text.SimpleDateFormat
import java.util.Date
import scalaj.http._
import org.apache.log4j.{Level, Logger}
import java.lang.management.MemoryUsage
import javax.management.MBeanAttributeInfo
import javax.management.MBeanInfo
import javax.management.MBeanServerConnection
import javax.management.ObjectInstance
import javax.management.ObjectName
import javax.management.openmbean.CompositeDataSupport
import javax.management.remote.JMXConnector
import javax.management.remote.JMXConnectorFactory
import javax.management.remote.JMXServiceURL
import scala.util.control._
object ConnectJmx {
  val jmx_info=scala.collection.mutable.Map[String,Any]()
  val scan_jmxson = (result1:scala.collection.mutable.Map[String,Any])=>{
    var result = result1
    var db_source = List[Map[String,String]]()
    var port = "8081"
    val ip = "192.168.10.129"
    val jmx = "10000"
    val http = result.getOrElse("http","")
    //val instance = result.get("instance").getOrElse("")
    val map = Map("jmx.remote.credentials"->Array("controlRole", "R&D"))
    val serviceURL = new JMXServiceURL(s"service:jmx:rmi:///jndi/rmi://$ip:$jmx/jmxrmi")
    try{
      //connect jmx
      val connector = JMXConnectorFactory.connect(serviceURL,map)
      val mbsc = connector.getMBeanServerConnection()

      //tomcat version
      val tomcat_version_objname = new ObjectName("Catalina:type=Server")
      result += ("tomcat_version"->mbsc.getAttribute(tomcat_version_objname, "serverInfo").toString)

//      //maxThreads  currentThreadCount
      val threadpool = new ObjectName("Catalina:type=ThreadPool,*")
      val set1 = mbsc.queryNames(threadpool, null)
      val loop1 = new Breaks
      loop1.breakable{
        for (obj <- set1){
          val objname = new ObjectName(obj.getCanonicalName)
          try{
            port = mbsc.getAttribute(objname, "port").toString
          }catch{
            case _ : Throwable =>;
          }
          if ( port == "8081"){
          result += ("maxThreads"->mbsc.getAttribute(objname, "maxThreads").toString)
          result += ("currentThreadCount"->mbsc.getAttribute(objname, "currentThreadCount").toString)
          loop1.break
         }
        }
      }
//
      //heap_memory_used heap_memory_max noheap_memory_used noheap_memory_max
      val heapObjName = new ObjectName("java.lang:type=Memory")
      val HeapMemoryUsage_tmp = mbsc.getAttribute(heapObjName,"HeapMemoryUsage")
      val heapMemoryUsage = MemoryUsage.from(HeapMemoryUsage_tmp.asInstanceOf[CompositeDataSupport])
      result += ("heap_memory_used"->heapMemoryUsage.getUsed.toString)
      result += ("heap_memory_max"->heapMemoryUsage.getMax.toString)
      val noHeapMemoryUsage_tmp = mbsc.getAttribute(heapObjName,"NonHeapMemoryUsage")
      val noheapMemoryUsage = MemoryUsage.from(noHeapMemoryUsage_tmp.asInstanceOf[CompositeDataSupport])
      result += ("noheap_memory_used"->noheapMemoryUsage.getUsed.toString)
      result += ("noheap_memory_max"->noheapMemoryUsage.getMax.toString)
//
//      //activeSessions maxActiveSessions
      val managerObjName = new ObjectName("Catalina:type=Manager,*")
      val set2 = mbsc.queryNames(managerObjName, null)
      val loop2 = new Breaks
      loop2.breakable{
        for (obj <- set2){
          val objname = new ObjectName(obj.getCanonicalName)
          var activeSessions = mbsc.getAttribute(objname,"activeSessions").toString
          result += ("maxActiveSessions"->mbsc.getAttribute(objname, "maxActiveSessions").toString)
          result += ("activeSessions"->activeSessions)
          if (activeSessions != "0"){
            loop2.break
          }
        }
      }
//
//      //c3p0 dbsource
//      val c3p0ObjName = new ObjectName("com.mchange.v2.c3p0:type=PooledDataSource*");
//      val set3 = mbsc.queryNames(c3p0ObjName,null)
//      val dbinfo_tmp = dbinfomap.get(ip+"_"+instance)
//      for (obj <- set3){
//        val objname = new ObjectName(obj.getCanonicalName());
//        val jdbcUrl = mbsc.getAttribute(obj,"jdbcUrl").toString
//        val user = mbsc.getAttribute(obj,"user").toString
//        //遍历dbinfo_tmp，获取name
//        val loop3 = new Breaks;
//        var dataSourceName = "default"
//        loop3.breakable{
//          for(db <- dbinfo_tmp){
//            if (db.user == user && db.url == jdbcUrl){
//              dataSourceName = db.name
//              loop3.break
//            }
//          }
//        }
//        val db = Map[String,String]("dbtype"->"c3p0",
//          "maxPoolSize"->mbsc.getAttribute(obj,"maxPoolSize").toString,
//          "minPoolSize"->mbsc.getAttribute(obj,"minPoolSize").toString,
//          "maxIdleTime"->mbsc.getAttribute(obj,"maxIdleTime").toString,
//          "numConnections"->mbsc.getAttribute(obj,"numConnections").toString,
//          "numBusyConnections"->mbsc.getAttribute(obj,"numBusyConnections").toString,
//          "numIdleConnections"->mbsc.getAttribute(obj,"numIdleConnections").toString,
//          "jdbcUrl"->jdbcUrl,
//          "user"->user,
//          "dataSourceName"->dataSourceName)
//        db_source = db :: db_source
//      }
//
      //tomcat.jdbc dbsource
      val argument = s"tomcat.jdbc:type=ConnectionPool,engine=Catalina,host=localhost,*"
      val jdbcObjName = new ObjectName(argument)
      val set4 = mbsc.queryNames(jdbcObjName,null)
      for (obj <- set4){
        val objname = new ObjectName(obj.getCanonicalName)
        val db = Map[String,String]("dbtype"->"tomcatjdbc",
          "MaxActive"->mbsc.getAttribute(obj,"MaxActive").toString,
          "Url"->mbsc.getAttribute(obj,"Url").toString,
          "MinIdle"->mbsc.getAttribute(obj,"MinIdle").toString,
          "MaxIdle"->mbsc.getAttribute(obj,"MaxIdle").toString,
          "Username"->mbsc.getAttribute(obj,"Username").toString,
          "NumActive"->mbsc.getAttribute(obj,"NumActive").toString,
          "name"->objname.getKeyProperty("name"))
        db_source = db :: db_source
      }
      result += ("db_source"->db_source)

      //ThreadCount  PeakThreadCount
      val ThreadingObjName = new ObjectName("java.lang:type=Threading")
      result += ("ThreadCount"->mbsc.getAttribute(ThreadingObjName, "ThreadCount").toString)
      result += ("PeakThreadCount"->mbsc.getAttribute(ThreadingObjName,"PeakThreadCount").toString)

      //close jmx connector
      connector.close()
      result += ("scan_jmx_result"->0); //0 表示成功
    }catch{
      case _ : Throwable => result += ("scan_jmx_result"->1); //1 表示失败
    }
    result
  }

  def main(args: Array[String]): Unit = {

    scan_jmxson(jmx_info).foreach(println)
  }
}
