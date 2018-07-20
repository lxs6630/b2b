package com.sgm.spark

import scalaj.http.Http

object info2influxdb {
  def main(args: Array[String]): Unit = {
//    val now=(new util.Random).nextInt(999)
//   // println(now)
//    val s=1493774517860L
//    println(s)
    val influxdb_url="http://192.168.10.129:8086/write?db=nginxlog_clear"
    val date="""info,name="li",sex="women" work="it",url="https://www.jingfengjiaoyu.com/templets/jingfeng_319/css/index.css",age=19 """
    Http(influxdb_url).postData(date).header("content-type", "application/json").asString.code
  }
}
