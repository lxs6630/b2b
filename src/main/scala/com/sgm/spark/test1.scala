package com.sgm.spark
//作用，类可以根据传入的参数不同执行不同的构造器
//class Stu8 {  /*每个类都有主构造器。主构造器并不以this方法定义，
//而是与类定义交织在一起没有显式定义主构造器则自动拥有一个无参的主构造器
//class Person ( val name:String, val aqe:Int){} (…)中的内容就是主构造器的参数*/
//  private[this] var  age=20    //私有构造器只能在当前使用
//  println("this is ".concat("主构建函器"))
//  def this(name:String){  //this 第一个从构造器
//    this //必须调用主构造器
//    println("this is ".concat("第一个构建函器"))
//    println("my name:"+name+",my age:"+this.age)
//  }
//  def this(weitht:Int){  //第二个从构造器
//    this
//    println("this is ".concat("第二个构建函器"))
//    println("my weitht is ".concat(weitht.toString))
//  }
//  def this (name: String,height: Int) { // 第三个个辅助构造器
//    this(name) //必须调用第一个辅助构造器
//    println("this is ".concat("第三个构建函器"))
//    println("my name:"+name+",my age:"+this.age+",my height:"+height)
//  }
//}
//
//object test1{
//  def main(arg:Array[String]){
//    var a = new Stu8  //只调用主构造器，不管调用哪个辅助构造器都会执行主构造器
//    println("-----------------1---------------------")
//    val b=new Stu8("leo") //根据传入类型调用第一个辅构造器
//    println("-----------------2---------------------")
//    val c=new Stu8(100) //根据传入类型调用第二个辅构造器，从构造器必定会调用到主构造器
//    println("------------------3--------------------")
//    val d=new Stu8("jack",18) //根据传入参数调用第三个辅构造器,同时也调用了第一个辅构造器，主构造器
//    println("--------------------------------------")
//
//  }
//}
/* this用构造函数。它演示了如何从其他构造函数调用构造函数。
必须确保必须放在构造函数中的第一个语句，同时调用其他构造函数，否则编译器会抛出错误。
 （带参数）*/
//构造器重载
//class Stu8(name:String,height:Int){
//    def this(name:String, age:Int,height:Int ){
//      this(name,height)
//      println("my name:"+name+",my age:"+age+";my height:"+height)
//    }
//  }
//
//  object Demo{
//    def main(arg:Array[String]){
//      var s = new Stu8("Maxsu",20,178)
//    }
//}

//    class Person(val name: String) {
//      def sayHello = println("Hello, I'm " + name)
//      def makeFriends(p: Person) {
//        sayHello
//        p.sayHello
//      }
//    }
//    class Student(name: String) extends Person(name)
//    class Dog(val name: String) { def sayHi = println("Wang, Wang, I'm " + name) }
//    implicit def dog2person(obj: Object): Person = if(obj.isInstanceOf[Dog]) {
//      val dog = obj.asInstanceOf[Dog]
//      println("OK")
//      new Person(dog.name) } else null //隐式转换
//    class Party[T <% Person](p1: T, p2: T){  //<%可以是Person或其子类，也可以是隐式转换
//      def play = p1.makeFriends(p2)
//    }
//    val dog=new Dog("jerry")
//    val tom=new Student("tom")
//    //tom.makeFriends(dog)
//    dog.makeFriends(tom)
//    val jack=new Student("jack")
//    val s=new Party(tom,dog)
//    s.play
//  }
object test1{
  class Person(val name: String) {
    def sayHello = println("Hello, I'm " + name)
    def makeFriends(p: Person) {
      sayHello
      p.sayHello
    }
  }
  class Student(name: String) extends Person(name)
  class Dog(val name: String) { def sayHi = println("Wang, Wang, I'm " + name) }
  implicit def dog2person(obj: Object): Person = if(obj.isInstanceOf[Dog]) {
    val dog = obj.asInstanceOf[Dog]
    new Person(dog.name) }else Nil//隐式转换
  class Party[T <% Person](p1: T, p2: T){  //<%可以是Person或其子类，也可以是隐式转换
    def play = p1.makeFriends(p2)
  }
  def main(args: Array[String]): Unit = {
        val dog=new Dog("jerry")
        val tom=new Person("tom")
        //tom.makeFriends(dog)
        //dog.makeFriends(tom)
        val jack=new Student("jack")
        val s=new Party(tom,dog)
        s.play
  }
}