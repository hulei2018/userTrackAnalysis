package com.usertrack.spark.util

import com.alibaba.fastjson.{JSONArray, JSONObject}
import com.usertrack.util.NumberUtils

/**
  * @author: hu lei
  * @description: ${description}
  * @date: 2019/8/4
  */
object JSONUtil {
       def mergeSessionAggrResultToJSONString(
                                               totalSessionCnt:Long,
                                               totalSessionSum:Double,
                                               invalidSessionCount:Long,
                                               preSessionLengthLevelSessionCount:Array[(String, Int)],
                                               dayAndHour2SessionCount: Array[((String, Int), Int)],
                                               dayAndHour2SessionLength: Array[((String, Int), Long)],
                                               invalidDayAndHourSessionLength: Array[((String, Int), Int)]
                                             ): String ={
         val tmpArray=new JSONArray()
         val obj=new JSONObject()
         obj.put("total_session_count",totalSessionCnt)
         obj.put("total_session_length",NumberUtils.formatDoubleOfNotUseGrouping(totalSessionSum))
         obj.put("invalid_session_count",invalidSessionCount)
         // 4. 添加每种会话类型中的会话数量
         tmpArray.clear()
         for((stype,scount)<-preSessionLengthLevelSessionCount){
           val obj1=new JSONObject()
           obj1.put("type",stype)
           obj1.put("count",scount)
           tmpArray.add(obj1)
         }
         obj.put("pre_session_type_of_count",tmpArray.clone())

         // 5. 添加每天每个小时中的会话数量
         tmpArray.clear()
         for(((day,hour),count)<-dayAndHour2SessionCount){
           val obj1=new JSONObject()
           obj1.put("day",day)
           obj1.put("hour",hour)
           obj1.put("count",count)
           tmpArray.add(obj1)
         }
         obj.put("pre_hour_of_session_count",tmpArray.clone())

         // 6. 添加每天每个小时的会话长度
         tmpArray.clear()
         for(((day,hour),length)<-dayAndHour2SessionLength){
           val obj1=new JSONObject()
           obj1.put("day",day)
           obj1.put("hour",hour)
           obj1.put("length",length)
           tmpArray.add(obj1)
         }
         obj.put("pre_hour_of_session_length",tmpArray.clone())

         // 7. 添加每天每个小时的无效会话数量
         tmpArray.clear()
         for(((day,hour),invalid)<-invalidDayAndHourSessionLength){
           val obj1=new JSONObject()
           obj1.put("day",day)
           obj1.put("hour",hour)
           obj1.put("invalid",invalid)
           tmpArray.add(obj1)
         }
         obj.put("pre_hour_of_invalid_session_count",tmpArray.clone())
         val json=obj.toJSONString
         json
       }

}
