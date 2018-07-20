package com.sgm.spark

object patternTest {
  def main(args: Array[String]): Unit = {
//    val  numberPattern="[1-5]+".r
//    val arraies=Array("123","45a","401","302","4444")
//    for(elment<-arraies){
//      println(numberPattern.findAllMatchIn(elment))
//    }
//val regex="""([0-9]+)([a-z]+)""".r//"""原生表达
//    val numPattern="[0-9]+".r
//    val numberPattern="""\s+[0-9]+\s+""".r
//    val t="[9]{2}[3-5]{3}".r
//    //findAllIn()方法返回遍历所有匹配项的迭代器
//    println(t.findAllIn("99345 Scala,22298 Spark"))
//    for(matchString <- t.findAllIn("99345 Scala,22298 Spark"))
//      println(matchString)
    //192.168.10.129
    val ipPattern="""(\d+\.\d+\.\d+\.\d+)(.*)(\d+/[A-Z,a-z]+/\d+\:\d+\:\d+\:\d+)(.*)([1-5]{1}[0-9]{2}) (\d+) (\"http.*\") (\".*\") (.*)""".r
    val s="""219.151.227.53 - - [03/May/2017:09:21:57 +0800] "POST /member.php?mod=logging&action=login&loginsubmit=yes&loginhash=LxvVI HTTP/1.1" 404 3861 "http://bbs.jingfengjiaoyu.com/member.php?mod=logging&action=login&viewlostpw=1" "Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0" 0.032"""
    val r="""219.151.227.55 - - [04/May/2017:09:22:05 +0800] "GET /home.php?mod=spacecp&ac=profile&op=info HTTP/1.1" 200 4546 "http://bbs.jingfengjiaoyu.com/memcp.php" "Mozilla/5.0 (Windows NT 5.1; rv:35.0) Gecko/20100101 Firefox/35.0"  0.018"""
    ipPattern.findAllIn(s).foreach(println)
    r match {
      case ipPattern(ip,none1,currentTime,none2,respCode,requestTime,url,none3,upstreamTIME)=>
        println(ip+"\t"+currentTime+"\t"+respCode+"\t"+requestTime+"\t"+url+"\t"+upstreamTIME+"\t")
      case _=>println("none")
    }
  }
}
