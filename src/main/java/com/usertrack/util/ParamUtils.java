package com.usertrack.util;

import com.alibaba.fastjson.JSONObject;
import com.usertrack.bean.Task;
import scala.Option;

public class ParamUtils {
    /**
     * 从命令行获取任务参数
     * @para args
     */
    public static Long getTaskIdFromArgs(String[] args){
        if(args!=null || args.length>0){
            try {
                return Long.parseLong(args[0]) ;
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 获取任务的参数
     *
     */
    public static JSONObject getTaskParam(Task task){
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
         if(taskParam!=null){
             return taskParam;
         }
         return null;
    }

    /**
     * 对json格式进行解析
     * @param jsonObject
     * @param field
     * @return 参数
     */
    public static Option<String> getParam(JSONObject jsonObject,String field){
        String value = jsonObject.getString(field);
        if(value==null || value.trim().isEmpty()){
            return Option.apply(null); //返回None
        }else {
            return Option.apply(value); //返回Some值
        }
    }
}
