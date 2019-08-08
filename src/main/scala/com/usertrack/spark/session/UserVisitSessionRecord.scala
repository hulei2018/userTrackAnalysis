package com.usertrack.spark.session

import com.alibaba.fastjson.JSONObject


/**
  * @author: hu lei
  * @description: ${description}
  * @date: 2019/8/1
  */
case class UserVisitSessionRecord(date:String,
                                  userId:Long,
                                  sessionId:String,
                                  pageId:Long,
                                  actionTime:String,
                                  searchKeyword:String,
                                  clickCategoryid:String,
                                  clickProductid:String,
                                  orderCategoryids:String,
                                  orderProductids:String,
                                  payCategoryids:String,
                                  payProductids:String,
                                  cityId:Int
                                 ) {
  def trans2JsonObject(): JSONObject ={
    val record = new JSONObject()
    record.put("date", date)
    record.put("user_id", userId)
    record.put("session_id", sessionId)
    record.put("page_id", pageId)
    record.put("action_time", actionTime)
    record.put("search_keyword", searchKeyword)
    record.put("click_category_id", clickCategoryid)
    record.put("click_product_id", clickProductid)
    record.put("order_category_ids", orderCategoryids)
    record.put("order_product_ids", orderProductids)
    record.put("pay_category_ids", payCategoryids)
    record.put("pay_product_ids", payProductids)
    record.put("city_id", cityId)
    record
  }
}
