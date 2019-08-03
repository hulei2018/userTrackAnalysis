package com.usertrack.spark.session

import com.alibaba.fastjson.JSONObject
import com.usertrack.conf.ConfigurationManager
import com.usertrack.dao.factory.TaskFactory
import com.usertrack.util.{DateUtils, ParamUtils}
import com.usertrack.constant.Constants
import com.usertrack.mock.MockDataUtils
import com.usertrack.spark.util.SparkUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: Jeremy Hu
  * @description: 用户行为分析
  * @date: 2019/7/21
  */
object UserVisitActionAnalyzerSpark {
  def main(args: Array[String]): Unit = {
    // 一、任务参数的过滤
    //1.1 获取任务参数
    val taskID = ParamUtils.getTaskIdFromArgs(args);
    //1.2 获取任务信息
    val task = if (taskID == null) {
      throw new IllegalArgumentException(s"不合法的参数输入${taskID}");
    } else {
      val taskDao = TaskFactory.taskFactory();
      taskDao.findTaskById(taskID);
    }
    // 3.获取任务的参数
    val taskParam = if (task == null) {
      throw new IllegalArgumentException(s"从数据库获取的ID没有对应taskParam${taskID}")
    } else {
      ParamUtils.getTaskParam(task);
    }
    if (taskParam == null || taskParam.isEmpty) {
      throw new IllegalArgumentException(s"不支持param数据为空的过滤${taskID}")
    }
    //二、获取上下文创建的环境
    // 2.1 创建spark的运行环境
    val appName = Constants.SPARK_APP_NAME + taskID;
    val islocal = ConfigurationManager.getBoolean(Constants.ISLOCAL)
    val conf = SparkUtils.generateSparkConf(appName, islocal, setSparkParam = (that: SparkConf) => {
      // 可以单独设置参数，以供使用
    })
    // 2.2sparkContext对象的构建
    val sc = SparkUtils.generateSparkContext(conf)

    //2.3 如果是本地的话读取数据，不用集成enableHive，如果提交到集群数据是存储在Hive中
    val spark = SparkUtils.loadDatas(islocal, appName, sc, generateMockData = (sc: SparkContext, spark: SparkSession) => {
      if (islocal) {
        MockDataUtils.mockData(sc, spark)
      }
    })

    //三、获取任务参数，并创建RDD
    val actionRDD: RDD[UserVisitSessionRecord] = filterData(spark, taskParam)

    //四、对session会话进行聚合，进行一下指标的统计
    val Session2RecordRdd: RDD[(String, Iterable[UserVisitSessionRecord])] = actionRDD.map(record => (record.sessionId, record))
      .groupByKey()
    //对session会话进行缓存，对后面的指标进行统计
    Session2RecordRdd.cache()

    //四、需求一代码
    /**
      * 用户的session聚合统计
      * 主要统计两个指标：会话数量&会话长度
      * 会话数量：sessionID的数量
      * 会话长度：一个会话中，最后一条访问记录的时间-第一条记录的访问数据
      * 具体的指标：
      * 1. 总的会话个数：过滤后RDD中sessionID的数量
      * 2. 总的会话长度(单位秒): 过滤后RDD中所有session的长度的和，不同sessionID会话的长度
      * 3. 无效会话数量：会话长度小于1秒的会话id数量
      * 4. 各个不同会话长度区间段中的会话数量
      * 0-4s/5-10s/11-30s/31-60s/1-3min/3-6min/6min+ ==> A\B\C\D\E\F\G
      * 5. 计算各个小时段的会话数量  即以小时为维度进行统计
      * 6. 计算各个小时段的会话长度
      * 7. 计算各个小时段的无效会话数量
      *
      * 注意：如果一个会话中，访问数据跨小时存在，eg：8:59访问第一个页面,9:02访问第二个页面；把这个会话计算在两个小时中(分别计算)
      **/

    //    1. 总的会话个数：过滤后RDD中sessionID的数量
    val totalsessionCount = Session2RecordRdd.count()

    //  2.总的会话长度(单位秒)
    val sessionLength = Session2RecordRdd.map {
      case (sessionID, records) => {
        val session_id = sessionID
        val actionTimeStamps: Iterable[Long] = records.map(r => {
          val actionTime = r.actionTime;
          val timeStamp = DateUtils.parseDate2Long(actionTime);
          timeStamp
        })
        val maxTimeStamp = actionTimeStamps.max
        val minTimeStamp = actionTimeStamps.min
        val length = maxTimeStamp - minTimeStamp
        (sessionID, length)
      }
    }
    sessionLength.cache()
    val sessionLengthSum = sessionLength.map(_._2).sum() / 1000
    //3. 无效会话数量：会话长度小于1秒的会话id数量
    val invalidSessionLength = sessionLength.filter(_._2 < 1000).count()
    // 4. 各个不同会话长度区间段中的会话数量
    val preSessionLengthLevelSessionCount: Array[(String, Int)] = sessionLength.map {
      case (_, length) => {
        val sessionLevel = {
          if (length < 5000) "A"
          else if (length < 11000) "B"
          else if (length < 31000) "C"
          else if (length < 60000) "D"
          else if (length < 180000) "E"
          else if (length < 360000) "F"
          else "G"
        }
        (sessionLevel, 1)
      }
    }
      .reduceByKey(_ + _)
      .collect()
    sessionLength.unpersist()

    //5. 计算各个小时段的会话数量
    val dayAndHour2SessionLengthRDD = Session2RecordRdd.flatMap {
      case (sessionID, record) => {
        //5.1 获取当前会话中的记录操作对应的时间
        val dayAndHourSessionID2TimeStamp = record.map(v => {
          val timestamp = DateUtils.parseDate2Long(v.actionTime)
          val day = DateUtils.parseLong2String(timestamp, DateUtils.DATE_FORMAT)
          val hour = DateUtils.getSpecificDateValueOfDateTypeEnum(timestamp, DateUtils.DateTypeEnum.HOUR)
          ((day, hour, v.sessionId), timestamp)
        })
        // 5.2 计算各个时间段各个会话中的会话长度
        val dayAndHourSessionLength = dayAndHourSessionID2TimeStamp.groupBy(_._1).map {
          case ((day, hour, sessionID), iter) => {
            val times = iter.map(_._2)
            val maxTimestamp = times.max
            val minTimestamp = times.min
            val length=maxTimestamp-minTimestamp
            ((day, hour, sessionID),length)
          }
        }
        // 5.3. 返回结果
        dayAndHourSessionLength
      }
    }
    dayAndHour2SessionLengthRDD.cache()
    //每个时段的会话个数
    val dayAndHour2SessionCount: Array[((String, Int), Int)] =dayAndHour2SessionLengthRDD.map(v=>((v._1._1,v._1._2),1))
      .reduceByKey(_+_)
      .collect()
    //每个小时内的会话的总长度
    val dayAndHour2SessionLength: Array[((String, Int), Long)] =dayAndHour2SessionLengthRDD.map(v=>((v._1._1,v._1._2),v._2))
      .reduceByKey(_+_)
      .collect()
    //每个小时内无效的会话个数
    val invalidDayAndHourSessionLength: Array[((String, Int), Int)] =dayAndHour2SessionLengthRDD.filter(_._2<1000)
      .map(tuple=>((tuple._1._1,tuple._1._2),1))
      .reduceByKey(_+_)
      .collect()
    dayAndHour2SessionLengthRDD.unpersist()

    // 保存模块一的结果
    this.saveSessionAggrResult(sc,taskID,totalsessionCount,sessionLengthSum,invalidSessionLength,preSessionLengthLevelSessionCount,dayAndHour2SessionCount,dayAndHour2SessionLength,invalidDayAndHourSessionLength)
  }

  def filterData(spark: SparkSession, taskParam: JSONObject): RDD[UserVisitSessionRecord] = {
    //获取过滤的参数
    val startDate = ParamUtils.getParam(taskParam, Constants.START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.END_DATE)
    val sex = ParamUtils.getParam(taskParam, Constants.SEX)
    val professions: Option[String] = ParamUtils.getParam(taskParam, Constants.PROFESSIONALS)
    //对professions参数进行处理
    val professionWhereStr = professions
      .map(v => {
        val str = v.split(",")
          .map(m => s"'{$m}'")
          .mkString("(", ",", ")")
        s"and ui.profession in ${str}"
      }).getOrElse("")

    //需要使用join条件
    val needJoinUserInfoTable: Option[Boolean] = if (sex.isDefined || professions.isDefined) Some(true) else None

    //hql语句过滤条件
    val hql =
      s"""
         |select
         |uva.*
         |from user_visit_action uva
         |${needJoinUserInfoTable.map(v => "join user_info ui on uva.user_id=ui.user_id").getOrElse("")}
         |where 1=1
         |${startDate.map(v => s"and uva.date>'${v}'").getOrElse("")}
         |${endDate.map(v => s"and uva.date<'${v}'").getOrElse("")}
         |${sex.map(v => s"and ui.sex=${v}").getOrElse("")}
         |${professionWhereStr}
         |
      """.stripMargin
    println(s"======\n${hql}\n=====")

    //将dataFrame转化为RDD
    val df = spark.sql(hql)
    val columns = Array("date", "user_id", "session_id", "page_id", "action_time", "search_keyword", "click_category_id", "click_product_id", "order_category_ids", "order_product_ids", "pay_category_ids", "pay_product_ids", "city_id")
    df.rdd.map(v => {
      val date = v.getAs[String](columns(0))
      val userId = v.getAs[Long](columns(1))
      val sessionId = v.getAs[String](columns(2))
      val pageId = v.getAs[Long](columns(3))
      val actionTime = v.getAs[String](columns(4))
      val searchKeyword = v.getAs[String](columns(5))
      val clickCategoryid = v.getAs[String](columns(6))
      val clickProductid = v.getAs[String](columns(7))
      val orderCategoryids = v.getAs[String](columns(8))
      val orderProductids = v.getAs[String](columns(9))
      val payCategoryids = v.getAs[String](columns(10))
      val payProductids = v.getAs[String](columns(11))
      val cityId = v.getAs[Int](columns(12))
      new UserVisitSessionRecord(date, userId, sessionId, pageId, actionTime, searchKeyword, clickCategoryid, clickProductid, orderCategoryids, orderProductids, payCategoryids, payProductids, cityId)
    })
  }

  //打印需求一的结果
  def saveSessionAggrResult(
                           sc:SparkContext,
                           taskID:Long,
                           totalSessionCnt:Long,
                           totalSessionSum:Double,
                           invalidSessionCount:Long,
                           preSessionLengthLevelSessionCount:Array[(String, Int)],
                           dayAndHour2SessionCount: Array[((String, Int), Int)],
                           dayAndHour2SessionLength: Array[((String, Int), Long)],
                           invalidDayAndHourSessionLength: Array[((String, Int), Int)]
                           ): Unit ={

  }
}
