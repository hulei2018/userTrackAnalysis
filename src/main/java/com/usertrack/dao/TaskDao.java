package com.usertrack.dao;

import com.usertrack.bean.Task;

public interface TaskDao {
    /**
     * @para taskID
     * @return task
     */
    public Task findTaskById(int taskid);
}
