package com.sgm.spark

object fab {
  def main(args: Array[String]): Unit = {
    def fab(num:Int):Int={
      val b = 1
      val a=0
      println("begin")
      Thread.sleep(10000)
      if(num<=1){
        1
      }else{
        fab(num-1) + fab(num-2)
      }

    }
    println(fab(2))
  }
}
