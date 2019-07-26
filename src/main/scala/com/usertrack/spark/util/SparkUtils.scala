package com.usertrack.spark.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * @author: hu lei
  * @description:
  * @date: 2019/7/25
  */
object SparkUtils {
  def generateSparkConf(apps: String, islocal: Boolean,setSparkParam:(SparkConf)=>Unit=Unit=>{}): SparkConf = {
    val conf=if(islocal){
      new SparkConf()
        .setAppName(apps)
        .setMaster("local[*]")

    }else{
      new SparkConf()
        .setAppName(apps)
    }
    conf.set("spark.sql.shuffle.partitions", "10")
    // RDD进行数据cache的时候，内存最多允许存储的大小（占executor的内存比例），默认0.6
    conf.set("spark.storage.memoryFraction", "0.6")
    // RDD进行shuffle的时候，shuffle数据写内存的阈值(占executor的内存比例），默认0.2
    conf.set("spark.shuffle.memoryFraction", "0.2")
    // 启动固定的内存分配模型，默认使用动态的内存模式
    conf.set("spark.memory.useLegacyMode", "true")
    setSparkParam(conf)
    conf
  }
  def generateSparkContext(conf:SparkConf):SparkContext=SparkContext.getOrCreate(conf)

  def loadDatas(islocal:Boolean,apps:String,sc:SparkContext,generateMockData:(SparkContext,SparkSession)=>Unit=(sc:SparkContext,spark:SparkSession)=>{}): SparkSession ={
    val spark=if(islocal){
      SparkSession.builder()
        .master("local[*]")
        .appName(apps)
        .getOrCreate()
          }else{
      SparkSession.builder()
        .appName(apps)
        .getOrCreate()
    }
    generateMockData(sc,spark)
    spark
  }
}
