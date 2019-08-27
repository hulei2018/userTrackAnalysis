package com.usertrack.spark.ad

import java.util.Properties

import com.usertrack.conf.ConfigurationManager
import com.usertrack.constant.Constants
import com.usertrack.spark.util.SparkUtils
import com.usertrack.util.DateUtils
import org.apache.calcite.avatica.MetaImpl.MetaIndexInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.{Failure, Success, Try}

/**
  * @author: Jeremy Hu
  * @description: ${description}
  * @date: 2019/8/11
  */
object AdClickRealTimeStateSpark {
  lazy val url=ConfigurationManager.getProperty(Constants.JDBC_URL)
  lazy val table="tb_black_users"
  lazy val properties={
    val prop=new Properties()
    prop.put("user",ConfigurationManager.getProperty(Constants.JDBC_USER))
    prop.put("password",ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    prop
  }
  // kafka中的数据分隔符
  val delimeter = " "

  def main(args: Array[String]): Unit = {
    // 一、上下文的构建
    // 1.1 获取相关变量
    val appName=Constants.SPARK_APP_NAME_AD
    val islocal=ConfigurationManager.getBoolean(Constants.ISLOCAL)

    // 1.2 创建上下文配置对象
    val conf=SparkUtils.generateSparkConf(appName,islocal,(that:SparkConf)=>{

    })

      //  1.3 SparkContext环境的构建
      val sc=SparkUtils.generateSparkContext(conf)

      // 1.4 SparkStreaming的构建
      // batchDuration: 指定批次产生间隔时间，一般要求：该值比批次的平均执行执行要大(大5%~10%)
      val ssc=new StreamingContext(sc,Seconds(10))

      // 1.5 设置checkpoint文件夹路径，实际工作中一定为hdfs路径，这里简化为本地磁盘路径
      val path=s"result/checkpoint/ad_${System.currentTimeMillis()}"
      ssc.checkpoint(path)

      val locationStrategy=LocationStrategies.PreferConsistent
      val topics=Constants.KAFKA_AD_TOPICS.split(",").toIterable
      val kafkaParams=Map[String,Object](
        "bootstrap.servers"->Constants.KAFKA_METADATA_BROKER_LIST,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id"->appName,
        "auto.offset.reset" -> "latest"
      )
      val offsets=Map[TopicPartition,Long](
        new TopicPartition(topics.toString(),0)->1000L,
        new TopicPartition(topics.toString(),1)->1000L,
        new TopicPartition(topics.toString(),2)->1000L
      )
      val consumerStrategy:ConsumerStrategy[String,String]=ConsumerStrategies.Subscribe(topics,kafkaParams,offsets)

      val inputStream =KafkaUtils.createDirectStream[String,String](ssc,locationStrategy,consumerStrategy)
        .map(_.value())

      // 三、数据格式转换(对于kafka中的数据可能需要进行一些转换操作，比如：类型转换)
      val formattedDStream= this.formattedAdRealTimeDStreamData(inputStream)

      // 四、黑名单数据过滤
      val filteredDStream = this.filterByBlackList(formattedDStream)

      // 五、黑名单实时/动态更新，近五分钟点击量超过100次就认为是黑名单
      this.dynamicUpdateBlackList(filteredDStream)

    // 六、实时累加统计广告点击量
    /**
      * 实时的统计每天 每个省份 每个城市  每个广告的点击点
      * 数据不涉及到去重操作，有一条算一条
      * ===> 以：日期+省份名称+城市名称+广告id为key，统计数据量的中
      **/
     val aggDStream=this.caculateRealTimeState(filteredDStream)

    // 七、获取各个省份TOP5的广告点击数据
    this.caculateProviceTop5Ad(aggDStream)

    // 八、获取最近一段时间的广告点击数据
    this.caculateClickCountByWindow(filteredDStream)

    ssc.start()
    ssc.awaitTermination()
    }


  /**
    * 将String类型转换为方便操作的数据类型. 数据的格式化操作
    *
    * @param inputStream
    */
  def formattedAdRealTimeDStreamData(inputStream: DStream[String]): DStream[AdClickRecord] ={
   inputStream.flatMap(v=>{
     val arr=v.split(delimeter)
       .map(_.trim)
       .filter(_.nonEmpty)

     Try{
       AdClickRecord(
         arr(0).toLong,
         arr(1),
         arr(2),
         arr(3).toInt,
         arr(4).toInt
       )
     }match {
       case Success(record)=>{
         Iterator.single(record).toIterable
       }
       case Failure(exception)=>{
          Iterator.empty.toIterable
       }
     }
   })
  }

  def filterByBlackList(formattedDStream:DStream[AdClickRecord]): DStream[AdClickRecord] ={
    formattedDStream.transform(rdd=>{
      val sc=rdd.sparkContext
      val spark=SparkUtils.loadDatas(true,Constants.SPARK_APP_NAME_AD,sc)
      val blackListRDD: RDD[(Int, Int)] =spark.read.jdbc(url,table,properties)
          .rdd.map(row=>{
        val user_id=row.getAs[Int]("user_id")
        val count = row.getAs[Int]("count")
        (user_id,count)
      })

      // 二、数据过滤
      val records=rdd.map(record=>(record.userID,record))
              .leftOuterJoin(blackListRDD)  //(userID,(record,count))
              .filter(_._2._2.isEmpty)
              .map(_._2._1)
      records
    })
  }

  def dynamicUpdateBlackList(filteredDStream:DStream[AdClickRecord]): Unit ={
    val combinerDStream=filteredDStream.map(record=>(record.userID,1))
            .reduceByKeyAndWindow(
              _+_,
              _-_,
              Minutes(5),
              Seconds(30)
            )

    //获取黑名单列表
    val blackDStream = combinerDStream
                .filter(_._2>100)
                .transform(record=>{
                  val sc=record.sparkContext
                  val whiteListRDD= sc.parallelize(0.until(1000))
                  val broadcastOfwhiteList=sc.broadcast(whiteListRDD.collect())
                  val blackListRDD: RDD[(Int, Int)] =record.filter(tuple=>(!broadcastOfwhiteList.value.contains(tuple._1)))
                  blackListRDD
                })
    blackDStream.foreachRDD(iter=>{
      val sc=iter.sparkContext
      val spark=SparkUtils.loadDatas(true,Constants.SPARK_APP_NAME_AD,sc)
      import spark.implicits._
      try{
        iter.map(x=>(x._1,x._2)).toDF("user_id","count")
          .write.mode("append").jdbc(url,table,properties)
      }catch {
        case e:Exception=>{
          println(e.getMessage)
        }
      }

    })
  }

  //日期+省份名称+城市名称+广告id为key
  def caculateRealTimeState(filteredDStream:DStream[AdClickRecord]): DStream[((String, String, String, Int), Long)] ={
   val aggrDStream=filteredDStream.map(record=>{
          val province=record.province
          val city=record.city
          val adID=record.adID
          val actionDate=DateUtils.parseLong2String(record.timestamp,"yyyy-MM-dd")
          ((actionDate,province,city,adID),1)
        }).reduceByKey(_+_)
          .updateStateByKey((value:Seq[Int],state:Option[Long])=>{
            val currentValue=value.sum
            val preState=state.getOrElse(0L)
            Some(currentValue+preState)
          })
    //输出结果
    aggrDStream.print(5)
    //返回输出结果
    aggrDStream
  }


  /**
    * 获取各个省份点击次数最多的Top5的广告数据
    * 最终需要的字段信息: 省份、广告id、点击次数、日期
    *
    * @param dstream
    */
  def caculateProviceTop5Ad(dstream:DStream[((String, String, String, Int), Long)]):Unit ={
    val top5ProvinceAdClickCountDStream =dstream.map{
              case((actionDate,province,_,adID),count)=>{
                ((actionDate,province,adID),count)
              }
            }
      .reduceByKey(_+_)
      .map{
        case(((actionDate,province,adID),count))=>{
          ((actionDate,province),(adID,count))
        }
      }
      .groupByKey()
      .flatMapValues(iter=>{
          iter.toList
          .sortWith(_._2>_._2)
          .slice(0,5)
        })
      .map{
        case((actionDate,province),(adID,count))=>{
          (((actionDate,province,adID),count))
        }
      }
    top5ProvinceAdClickCountDStream.print(5)
  }

/**
  * 目的：是为了看广告的点击趋势的
  * 实时统计最近十分钟的广告点击量
  * 使用window函数进行分析
  * -1. 窗口大小： 指定每个批次执行的时候，处理的数据量是最近十分钟的数据
  * window interval: 60 * 10 = 600s
  * -2. 滑动大小： 指定批次生成的间隔时间为一分钟，即一分钟产生一个待执行的批次
  * slider interval: 1 * 60 = 60s
  *
  * @param dstream
  */

  def caculateClickCountByWindow(dstream:DStream[AdClickRecord]): Unit ={
    val aggDstream =dstream.map(record=>(record.adID,1))
          .reduceByKeyAndWindow(
            _+_,
            _-_,
            Minutes(10),
            Minutes(1)
          )

    // 3. 为了能够体现点击的趋势变化，直白来讲就是画图的时候可以画一个曲线图(与时间相光的); 所以需要在dstream中的数据中添加一个时间种类的属性 ==> 可以考虑将批次时间添加到数据中
    val adClickCount= aggDstream.transform((rdd,time)=>{
        val dateStr=DateUtils.parseLong2String(time.milliseconds,"yyyyMMddHHmmss")
        rdd.map{
          case(adID,count)=>{
            (adID,count,dateStr)
          }
        }
      })
    // 4. 数据保存MySQL
    adClickCount.print(5)
  }

}
case class AdClickRecord(
                          timestamp: Long,
                          province: String,
                          city: String,
                          userID: Int,
                          adID: Int
                        )
