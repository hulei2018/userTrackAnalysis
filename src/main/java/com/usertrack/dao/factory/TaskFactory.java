package com.usertrack.dao.factory;

import com.usertrack.dao.TaskDao;
import com.usertrack.dao.impl.TaskDaoImpl;

public class TaskFactory {
    /**
     * 创建一个工厂用于生产TaskDaoImpl
     */
    public static TaskDao taskFactory(){
        return new TaskDaoImpl();
    }
}
