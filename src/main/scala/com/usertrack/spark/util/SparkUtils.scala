package com.usertrack.spark.util

import org.apache.spark.SparkConf

/**
  * @author: Jeremy Hu
  * @description:
  * @date: 2019/7/25
  */
object SparkUtils {
  def generateSparkConf(apps: String, islocal: Boolean): SparkConf = {
    val conf = if (islocal) {
      new SparkConf()
        .setAppName(apps)
        .setMaster("local[*]")
    } else {
      new SparkConf()
        .setAppName(apps)
    }
    conf.set("spark.sql.shuffle.partitions", "10")
    // RDD进行数据cache的时候，内存最多允许存储的大小（占executor的内存比例），默认0.6
    // 如果内存不够，可能有部分数据不会进行cache(CacheManager会对cache的RDD数据进行管理操作<删除不会用的RDD缓存>)
    conf.set("spark.memory.fraction","0.6")
    conf.set("","")
  }
}
