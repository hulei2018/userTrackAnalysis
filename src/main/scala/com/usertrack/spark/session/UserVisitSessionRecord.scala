package com.usertrack.spark.session

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

}
