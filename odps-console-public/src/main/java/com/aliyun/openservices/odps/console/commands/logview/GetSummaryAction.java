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

package com.aliyun.openservices.odps.console.commands.logview;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class GetSummaryAction extends LogViewBaseAction {

  public static final String ACTION_NAME = "sum";

  private String taskName;

  @SuppressWarnings("static-access")
  public Options getOptions() {
    Options options = super.getOptions();
    options.addOption(OptionBuilder.withArgName("task name").hasArg().create('t'));
    return options;
  }

  public void configure(CommandLine cl) throws LogViewArgumentException {
    super.configure(cl);
    if (cl.hasOption('t')) {
      taskName = cl.getOptionValue('t');
    }
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Instance inst = ctx.getInstance();
    if (taskName == null) {
      taskName = ctx.getTask();
      if (taskName == null) {
        taskName = deduceTaskName(inst);
      }
    }
    ctx.setTask(taskName);
    TaskSummary sum = inst.getTaskSummary(taskName);
    getWriter().writeResult(sum.toString());
  }

  @Override
  public String getActionName() {
    return ACTION_NAME;
  }

}
