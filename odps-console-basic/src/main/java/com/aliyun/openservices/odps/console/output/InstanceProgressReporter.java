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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.StageProgress;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Task;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

/**
 * @author shuman.gansm 此类为了兼容console的输出
 * */
public class InstanceProgressReporter implements IProgressReporter {

  ExecutionContext context;
  private Odps odps;

  public InstanceProgressReporter(ExecutionContext context, Odps odps) {
    super();
    this.context = context;
    this.odps = odps;
  }

  // @Override
  public void report(InstanceProgress progress) {

    switch (progress.getStage()) {
    case CREATE_INSTANCE:
      reportInstance(progress.getInstance(), progress.getTask());
    break;
    case REPORT_TASK_PROGRESS:
      reportProgress(progress.getTaskProgress());
    break;
    case FINISH:
      reportResult(progress.getTaskStatus(), progress.getSummary(), progress.getResult(),
          progress.getTask(), progress.getInstance());
    break;
    }
  }

  private void reportInstance(Instance instance, Task task) {

    DefaultOutputWriter writer = context.getOutputWriter();
    // 输出一个空行,让HiveUT可以work
    writer.writeResult("");

    writer.writeError("ID = " + instance.getId());

    String logviewUrl = ODPSConsoleUtils.generateLogView(odps, instance, context);
    if (StringUtils.isNullOrEmpty(logviewUrl)) {
      return;
    }
    writer.writeError("Log view:");
    writer.writeError(logviewUrl);
  }

  private void reportProgress(List<StageProgress> progress) {

    DefaultOutputWriter writer = context.getOutputWriter();

    // 至少有一个job
    //List<StageProgress> progress = instance.getTaskProgress();
    if (progress == null || progress.size() == 0 || progress.get(progress.size() - 1) == null) {
      return;
    }

    writer.writeError(Instance.getStageProgressFormattedString(progress));
  }

  private void reportResult(TaskStatus taskStatus, TaskSummary taskSummary, String queryResult,
      Task task, Instance instance) {

    // 输出summary信息
    try {

      if (taskSummary == null || "".equals(taskSummary.toString().trim())) {
        return;
      }
      // print Summary
      String summary = taskSummary.getSummaryText();

      context.getOutputWriter().writeError("Summary:");
      context.getOutputWriter().writeError(summary);

    } catch (Exception e) {
      context.getOutputWriter().writeError("can not get summary. " + e.getMessage());
    }

  }

}
