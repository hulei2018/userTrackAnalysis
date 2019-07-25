package com.usertrack.util;

import com.alibaba.fastjson.JSONObject;
import com.usertrack.bean.Task;

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
}
