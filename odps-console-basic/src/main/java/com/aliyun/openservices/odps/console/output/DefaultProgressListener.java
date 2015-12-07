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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Instance.StageProgress;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Task;
import com.aliyun.openservices.odps.console.output.InstanceProgress.InstanceStage;

/**
 * @author shuman.gansm
 * 
 * **/
public class DefaultProgressListener implements ProgressListener {

    IProgressReporter reporter;
    InstanceProgress progress;
    List<Task> tasks;
    
    //正在运行的task
    private String rName;
    
    public DefaultProgressListener(InstanceProgress progress, IProgressReporter reporter, List<Task> tasks) {
        super();
        this.reporter = reporter;
        this.progress = progress;
        this.tasks = tasks;
    }

    @Override
    public Iterable<String> getTaskNames() {
        List<String> taskNames = new ArrayList<String>();
        
        rName = null;
        if (tasks != null && tasks.size() == 1){
            //一个task是当前最多的情况，为了节省一次getTaskStatus的requst，缓存一下，这个请求访问很频繁
            rName = tasks.get(0).getName();
            taskNames.add(rName);
        }else{
            try {
                Map<String, TaskStatus> taskStatuses = progress.getInstance().getTaskStatus();
                for (String name : taskStatuses.keySet()){
                    //只report第一个找到的running的task
                    if (TaskStatus.Status.RUNNING.equals(taskStatuses.get(name).getStatus())){
                        rName = name;
                        taskNames.add(rName);
                        break;
                    }
                }
            } catch (Exception e) {
                //如果取不到, 本次不report出去
            }
        }
        
        return taskNames;
    }

    @Override
    public void report(Map<String, List<StageProgress>> progresses) {

        if (rName == null){
            //拿不到运行的task，就不report
            return;
        }
        
        progress.setStage(InstanceStage.REPORT_TASK_PROGRESS);
        progress.setTaskProgress(progresses.get(rName));
        // 处理instance的执行进度
        reporter.report(progress);
        
    }

}
