package com.usertrack.spark.session

import com.usertrack.dao.factory.TaskFactory
import com.usertrack.util.ParamUtils
import com.usertrack.constant.Constants

/**
  * @author: Jeremy Hu
  * @description: 用户行为分析
  * @date: 2019/7/21
  */
object UserVisitActionAnalyzerSpark {
  def main(args: Array[String]): Unit = {
  // 一、任务参数的过滤
   //1、获取任务参数
    val taskID=ParamUtils.getTaskIdFromArgs(args);
    //2.获取任务信息
    val task =if(taskID==null){
          throw new IllegalArgumentException(s"不合法的参数输入${taskID}");
        }else{
          val taskDao=TaskFactory.taskFactory();
          taskDao.findTaskById(taskID);
        }
     // 3.获取任务的参数
     val taskParam= if(task==null){
             throw new IllegalArgumentException(s"从数据库获取的ID没有对应taskParam${taskID}")
           }else{
             ParamUtils.getTaskParam(task);
           }
    if(taskParam==null||taskParam.isEmpty){
      throw new IllegalArgumentException(s"不支持param数据为空的过滤${taskID}")
    }
    // 4.创建spark的运行环境
    val appName=Constants.SPARK_APP_NAME+taskID;
    val islocal=
    val spark=

  }
}
