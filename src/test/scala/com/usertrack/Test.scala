package com.usertrack

import com.usertrack.util.DateUtils

/**
  * @author: hu lei
  * @description: ${description}
  * @date: 2019/8/2
  */
object Test {
  def main(args: Array[String]): Unit = {
//    val time: Long =System.currentTimeMillis()
//    println(DateParse.getDate(time))
    val current="2019-08-02 23:06:40"
    println(DateParse.getCureentTime(current)/1000)
  }
}
