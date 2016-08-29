package org.tonypacer.domain;

import java.io.Serializable;

/**
 * Created by XuanYu
 */
public class Task implements Serializable {

    private static final long serialVersionUID = 3518776796426921776L;

    private long taskId;
    private String taskName;
    private String createTime;
    private String startTime;
    private String finishTime;
    private String taskType;
    private String taskStatus;
    private String taskParam;

    public Task(){ }

    public Task(long taskId, String taskName, String createTime,
         String startTime, String finishTime, String taskType,
         String taskStatus, String taskParam){
        this.setTaskId(taskId);
        this.setTaskName(taskName);
        this.setCreateTime(createTime);
        this.setStartTime(startTime);
        this.setFinishTime(finishTime);
        this.setTaskType(taskType);
        this.setTaskStatus(taskStatus);
        this.setTaskParam(taskParam);
    }

    public long getTaskId() {
        return taskId;
    }
    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }
    public String getTaskName() {
        return taskName;
    }
    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
    public String getCreateTime() {
        return createTime;
    }
    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
    public String getStartTime() {
        return startTime;
    }
    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }
    public String getFinishTime() {
        return finishTime;
    }
    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }
    public String getTaskType() {
        return taskType;
    }
    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }
    public String getTaskStatus() {
        return taskStatus;
    }
    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }
    public String getTaskParam() {
        return taskParam;
    }
    public void setTaskParam(String taskParam) {
        this.taskParam = taskParam;
    }

}
