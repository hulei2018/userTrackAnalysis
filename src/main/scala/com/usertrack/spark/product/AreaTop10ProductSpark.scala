package com.usertrack.spark.product

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSON
import com.usertrack.conf.ConfigurationManager
import com.usertrack.constant.Constants
import com.usertrack.dao.factory.TaskFactory
import com.usertrack.jdbc.Jdbc_Help
import com.usertrack.mock.MockDataUtils
import com.usertrack.spark.util.SparkUtils
import com.usertrack.util.ParamUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}


/**
  * @author: hu lei
  * @description: ${description}
  * @date: 2019/8/10
  */
object AreaTop10ProductSpark {
  lazy val url=ConfigurationManager.getProperty(Constants.JDBC_URL)
  lazy val table="city_info"
  lazy val properties={
    val prop=new Properties()
    prop.put("user",ConfigurationManager.getProperty(Constants.JDBC_USER))
    prop.put("password",ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    prop
  }
  def main(args: Array[String]): Unit = {
    // 一、任务参数的过滤
    //1.1 获取任务参数
    val taskID=ParamUtils.getTaskIdFromArgs(args)
    //1.2 获取任务信息
    val task=if(taskID==null){
      throw new IllegalArgumentException(s"不合法的参数输入${taskID}")
    }else{
      val taskDao=TaskFactory.taskFactory()
      taskDao.findTaskById(taskID)
    }
    // 3.获取任务的参数
    val taskParam=if(task==null){
      throw new IllegalArgumentException(s"不合法的参数输入${taskID}")
    }else{
      ParamUtils.getTaskParam(task)
    }
    if (taskParam == null || taskParam.isEmpty) {
      throw new IllegalArgumentException(s"不支持param数据为空的过滤${taskID}")
    }

    //二、获取上下文创建的环境
    // 2.1 创建spark的运行环境
    val appName=Constants.SPARK_APP_NAME_PRODUCT+taskID
    val islocal=ConfigurationManager.getBoolean(Constants.ISLOCAL)
    val conf=SparkUtils.generateSparkConf(appName,islocal, (that:SparkConf)=>{

    })

    // 2.2sparkContext对象的构建
    val sc = SparkUtils.generateSparkContext(conf)

    //2.3 如果是本地的话读取数据，不用集成enableHive，如果提交到集群数据是存储在Hive中
    val spark=SparkUtils.loadDatas(islocal,appName,sc,generateMockData =(sc,spark)=>{
      // 模拟数据构建: user_visit_action和user_info的数据
      if(islocal){
        MockDataUtils.mockData(sc,spark)
        MockDataUtils.loadProductInfoMockData(sc,spark)
      }
    })

    // 三、数据过滤（不需要返回RDD以及过滤参数有可能不同）
    val tmp_basic_user_visit_action=this.filterDataByTaskParamAndRegisterTmpTable(spark, taskParam)

    // 四、读取关系型数据库中的city_info城市信息表的数据，并注册成为临时表
    val tmp_city_info=this.fetchCityInfoDataAndRegisterTmpTable(spark)
    val broadcast_city_info =spark.sparkContext.broadcast(tmp_city_info)

    // 五、将基础的用户访问数据和城市信息表数据进行join操作，并注册成为临时表
    val tmp_basic_product_city_info=this.registerTableMergeUserVisitActionAndCityInfoData(tmp_basic_user_visit_action,broadcast_city_info)
    broadcast_city_info.unpersist(true)

    // 六、统计各个区域、各个商品的点击次数(area、product_id、click_count)
    val tmp_area_product_click_count=this.registerAreaProductClickCountTable(tmp_basic_product_city_info)

    //七、分组排序TopN ==> 对各个区域的点击数据进行分组，然后对每组数据获取点击次数最多的前10个商品
    val tmp_area_top10_product_click_count=this.registerAreaTop10ProductClickCountTable(tmp_area_product_click_count)

    // 八、合并商品信息，得到最终的结果表
    val tmp_result=this.registerMergeTop10AndProductInfoTable(spark, tmp_area_product_click_count)

    // 九、结果输出
    this.saveResult(taskID,tmp_result)

  }


  def filterDataByTaskParamAndRegisterTmpTable(spark:SparkSession,taskParam:JSONObject): DataFrame ={
    //获取过滤的参数
    val startDate = ParamUtils.getParam(taskParam, Constants.START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.END_DATE)
    val sex = ParamUtils.getParam(taskParam, Constants.SEX)
    val professions: Option[String] = ParamUtils.getParam(taskParam, Constants.PROFESSIONALS)
    //对professions参数进行处理
    val professionWhereStr = professions
      .map(v => {
        val str = v.split(",")
          .map(m => s"'${m}'")
          .mkString("(", ",", ")")
        s"and ui.professional in ${str}"
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
         |${startDate.map(v => s"and uva.date>='${v}'").getOrElse("")}
         |${endDate.map(v => s"and uva.date<='${v}'").getOrElse("")}
         |${sex.map(v => s"and ui.sex='${v}'").getOrElse("")}
         |${professionWhereStr}
         |
      """.stripMargin
    println(s"======\n${hql}\n=====")

    //将dataFrame转化为RDD
    val tmp_basic_user_visit_action = spark.sql(hql)
    tmp_basic_user_visit_action.show()
    tmp_basic_user_visit_action
  }

  def fetchCityInfoDataAndRegisterTmpTable(spark:SparkSession): DataFrame ={
    val tmp_city_info=spark.read.jdbc(url, table, properties)
    tmp_city_info
  }

  def registerTableMergeUserVisitActionAndCityInfoData(tmp_basic_user_visit_action:DataFrame,broadcast_city_info: Broadcast[DataFrame]): DataFrame ={
    val tmp_basic_product_city_info=tmp_basic_user_visit_action
      .join(broadcast_city_info.value,"city_id")
      .selectExpr("city_id","click_product_id","city_name","area","province_name")
    tmp_basic_product_city_info
  }

  def registerAreaProductClickCountTable(tmp_basic_product_city_info:DataFrame): DataFrame ={
    val concatCityidAndNameudf=udf{ (city_id:String, city_name:String)=>{
      val concat_id_city=s"${city_id}:${city_name}"
         concat_id_city
    }}

    val a= tmp_basic_product_city_info.withColumn("concat_id_name",concatCityidAndNameudf(col("city_id"),col("city_name")))
      .groupBy("area","click_product_id")
        .agg(count("*").as("click_count"),collect_list("concat_id_name").as("concat_id_name_list"))
//        .selectExpr("area","click_product_id","click_count","cast(concat_id_name_list as String)")
    a
  }

  def registerAreaTop10ProductClickCountTable(tmp_area_product_click_count:DataFrame): DataFrame ={
    val w=Window.partitionBy("area").orderBy(col("click_count").desc)
    val tmp_area_top10_product_click_count=tmp_area_product_click_count.withColumn("rn",row_number().over(w)).where("rn<=10")
    tmp_area_top10_product_click_count.show()
    tmp_area_top10_product_click_count
  }

/**
  * 合并商品信息
  *
  * @param df
  */
  def registerMergeTop10AndProductInfoTable(spark:SparkSession,df:DataFrame): DataFrame ={

    val top10Product=df.select(col("area"),when(col("area").equalTo("华东"),"A")
      .when(col("area").equalTo("华南"),"A")
      .when(col("area").equalTo("华北"),"B")
      .when(col("area").equalTo("华中"),"B")
      .when(col("area").equalTo("东北"),"C")
        .otherwise("D").alias("area_level"),col("click_product_id")
      ,col("click_count"),col("concat_id_name_list")
    )

    val fetch_product_type_from_json=udf{(extend_info:String)=>{
     val value=JSON.parseObject(extend_info)
       .getString("product_type")

      if(value==null||value.isEmpty){
       "未知商品"
      }else{
        value match {
          case "1"=>"第三方商品"
          case "0"=>"自营商品"
          case _ =>"未知商品"
        }
      }

    }}
    val productInfo=spark.sql("select * from product_info")
    val productInfoTmp= productInfo.withColumn("product_type",fetch_product_type_from_json(col("extend_info")))
    val tmp_result=top10Product.join(productInfoTmp,top10Product("click_product_id")=== productInfoTmp("product_id"))
      .selectExpr("area_level","click_product_id","area","click_count","concat_id_name_list","product_name","product_type")
    tmp_result

  }

  def saveResult(taskID:Long,df:DataFrame): Unit ={
    // 一般数据输输出到关系型数据库中
    /**
      * 数据输出到关系型数据库的方式：
      * 1. 将数据转换为RDD，然后RDD进行数据输出
      * --a. rdd调用foreachPartition定义数据输出代码进行输出操作
      * --b. rdd调用saveXXX方式使用自定义的OutputFormat进行数据输出
      * 2. 使用DataFrame的write.jdbc进行数据输出 ==> 不支持Insert Or Update操作
      */
    df.rdd.map(v=>{
      val area_level=v.getAs[String]("area_level")
      val product_id=v.getAs[String]("click_product_id")
      val area=v.getAs[String]("area")
      val count=v.getAs[Long]("click_count")
      val city_infos=v.getAs[Array[String]]("concat_id_name_list").toString
      val product_name=v.getAs[String]("product_name")
      val product_type=v.getAs[String]("product_type")
      (area,product_id,area_level,count,city_infos,product_name,product_type)
    }).foreachPartition(iter=>{
      val jdbc_helper=Jdbc_Help.getIntants
      Try{
        val con=jdbc_helper.getConnection
        val oldcommit=con.getAutoCommit
        con.setAutoCommit(false)
        val sql="insert into tb_area_top10_product(task_id,area,product_id,area_level,count,city_infos,product_name,product_type)  values(?,?,?,?,?,?,?,?)"
        val pstm=con.prepareStatement(sql)
        var recordCount = 0
        iter.foreach{case(area,product_id,area_level,count,city_infos,product_name,product_type)=>{
          pstm.setLong(1,taskID)
          pstm.setString(2,area)
          pstm.setString(3,product_id)
          pstm.setString(4,area_level)
          pstm.setLong(5,count)
          pstm.setString(6,city_infos)
          pstm.setString(7,product_name)
          pstm.setString(8,product_type)
          pstm.addBatch()
          recordCount+=1
          if(recordCount%500==0){
            pstm.executeBatch()
            con.commit()
          }
        }}
        pstm.executeBatch()
        con.commit()
        (oldcommit,con)
      } match {
        case Failure(exception) => {
          jdbc_helper.returnConnetion(null)
          throw exception
        }
        case Success((oldcommit,con)) =>{
          jdbc_helper.returnConnetion(con)
          con.setAutoCommit(oldcommit)
        }
      }
    })
    }
}
