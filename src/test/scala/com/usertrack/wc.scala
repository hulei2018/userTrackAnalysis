package com.usertrack

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: hu lei
  * @description: ${description}
  * @date: 2019/8/4
  */
object wc {
  def main(args: Array[String]): Unit = {
    val arr=Seq("A","B","A","C","D","C")
    val conf=new SparkConf()
      .setAppName("My Test")
      .setMaster("local[*]")

    val sc=SparkContext.getOrCreate(conf)
    val textFile=sc.makeRDD(arr)
    val wc=textFile.flatMap(_.split(","))
      .map((_,1))

//      .aggregateByKey(0)(
//        (u,v)=>u+v,
//        (u1,u2)=>u1+u2
//      )
//      .collect()
//      .foreach(println(_))

  }
}
