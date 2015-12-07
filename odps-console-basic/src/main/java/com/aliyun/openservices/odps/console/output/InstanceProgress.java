/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.openservices.odps.console.output;

import java.util.List;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.StageProgress;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.Task;

/**
 * @author shuman.gansm
 * 
 * 按不同的stage 汇报不同的的运行状态
 * */
public class InstanceProgress {
    
    public static enum InstanceStage{
        //不同的阶段,用户可以做不同的处理
        //生成ID
        CREATE_INSTANCE,
        //正在执行,汇报进度
        REPORT_TASK_PROGRESS,
        //结束
        FINISH;
    }
    
    //instance执行的阶段
    private InstanceStage stage;
    private Instance instance;
    private Task task;
    private List<StageProgress> taskProgress;
    private TaskStatus taskStatus;
    private TaskSummary summary;
    private String result;
    
    /**
     * @param stage 当前instance所处的阶段
     * @param instance 当前的instance
     * */
    public InstanceProgress(InstanceStage stage, Instance instance, Task task) {
        super();
        this.stage = stage;
        this.instance = instance;
        this.task = task;
    }
    public InstanceStage getStage() {
        return stage;
    }
    public Instance getInstance() {
        return instance;
    }
    public Task getTask() {
        return task;
    }
    public void setStage(InstanceStage stage) {
        this.stage = stage;
    }
    public void setInstance(Instance instance) {
        this.instance = instance;
    }
    public void setTask(Task task) {
        this.task = task;
    }
    public List<StageProgress> getTaskProgress() {
        return taskProgress;
    }
    public void setTaskProgress(List<StageProgress> taskProgress) {
        this.taskProgress = taskProgress;
    }
    public TaskStatus getTaskStatus() {
        return taskStatus;
    }
    public void setTaskStatus(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }
    public TaskSummary getSummary() {
        return summary;
    }
    public void setSummary(TaskSummary summary) {
        this.summary = summary;
    }
    public String getResult() {
        return result;
    }
    public void setResult(String result) {
        this.result = result;
    }
    
}
