package com.sgm.spark


import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtil {
  val curent_day=FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
  val targe_fomat=FastDateFormat.getInstance("yyyyMMddHHmmss")
  def getTime(time:String)={    //转化为时间戳
    curent_day.parse(time).getTime
  }
  def parseTime(time:String)={
    targe_fomat.format(new Date(getTime(time)))
  }
  def main(args: Array[String]): Unit = {
    println(parseTime("04/MAY/2017:09:22:05"))
  }
}
