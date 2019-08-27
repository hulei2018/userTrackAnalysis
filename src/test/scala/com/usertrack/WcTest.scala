package com.usertrack
import scala.collection.mutable.Map


/**
  * @author: hu lei
  * @description: ${description}
  * @date: 2019/8/11
  */
object WcTest {
  def main(args: Array[String]): Unit = {
    var hMap = Map[Char, Int]()
    val str = "asddfgsg"
   str.map(s=> {
     hMap.get(s) match {
        case Some(x) => hMap(s) = x + 1
        case None => hMap ++= Map(s -> 1)
      }
      hMap
    })
    println(hMap)
  }
}
