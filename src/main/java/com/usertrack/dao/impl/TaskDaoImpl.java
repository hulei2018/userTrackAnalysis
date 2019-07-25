package com.usertrack.dao.impl;

import com.usertrack.bean.Task;
import com.usertrack.dao.TaskDao;
import com.usertrack.jdbc.Jdbc_Help;

import java.sql.ResultSet;

public class TaskDaoImpl implements TaskDao {
    Task task=null;
    @Override
    public Task findTaskById(Long taskid) {
        String sql = "select * from tb_task where task_id = ?";
        Object[] params = new Object[]{taskid};
        Jdbc_Help inst = Jdbc_Help.getIntants();
        inst.executionQury(sql, params, new Jdbc_Help.CallBack() {
            @Override
            public void process(ResultSet res) throws Exception {
                while(res.next()){
                    task=new Task();
                    long taskId = res.getLong(1);
                    String taskName = res.getString(2);
                    String createTime = res.getString(3);
                    String startTime = res.getString(4);
                    String finishTime = res.getString(5);
                    String taskType = res.getString(6);
                    String taskStatus = res.getString(7);
                    String taskParam = res.getString(8);

                    task.setTaskId(taskId);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });
        return task;
    }
}
