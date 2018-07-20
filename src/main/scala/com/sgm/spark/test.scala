package com.sgm.spark

object test{
  def main(args: Array[String]): Unit = {
    class Person(val name:String)
    class Pet(val name:String)
    class Student(name:String) extends Person(name)
    class Soldier(name:String) extends Person(name)
    class DisabledPopel(name:String) extends Person(name)
    class Dog(name:String) extends Pet(name)
    class SpecialPerson(val name:String)
    class SpecialTicketWindow(obj:SpecialPerson) {
      def buyTicket: Unit = {
        print(obj.name+",you buy ticket sucess")
      }
    }
    implicit def Person2SpecialPerson(obj:Object):SpecialPerson= {  //指定返回类型为需要转换成的类型
      if (obj.getClass == classOf[Student]) {
        val stu = obj.asInstanceOf[Student]
        new SpecialPerson(stu.name)
      } else null
    }
    val tom=new Student("tom")
    val s=new SpecialTicketWindow(tom)
      s.buyTicket
  }
}