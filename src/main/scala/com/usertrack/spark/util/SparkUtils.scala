package com.usertrack.spark.util

import org.apache.spark.sql.SparkSession

/**
  * @author: hu lei
  * @description:
  * @date: 2019/7/25
  */
object SparkUtils {
  def generateSparkConf(apps: String, islocal: Boolean): SparkSession = {
    val spark = if (islocal) {
      SparkSession.builder()
        .appName(apps)
        .master("local[*]")
        .enableHiveSupport()
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
        .getOrCreate()
    } else {
      SparkSession.builder()
        .appName(apps)
        .enableHiveSupport()
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
        .getOrCreate()
    }

  }
}
