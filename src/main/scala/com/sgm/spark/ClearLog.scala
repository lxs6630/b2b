package com.sgm.spark

/**
  * 清洗后的日志
  * @param time 访问时间
  * @param ip  访问地址
  * @param statusCode 状态码
  * @param url  访问URL
  */
case class ClearLog(time:String,ip:String,statusCode:Int,url:String)
case class DataCount(status:Int,statusCount:Int)