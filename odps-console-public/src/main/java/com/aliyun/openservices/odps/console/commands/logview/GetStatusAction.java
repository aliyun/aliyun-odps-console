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

import java.util.Map;

import org.apache.commons.cli.CommandLine;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class GetStatusAction extends LogViewBaseAction {

  public static final String ACTION_NAME = "status";

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Instance inst = ctx.getInstance();
    Map<String, TaskStatus> status = inst.getTaskStatus();
    getWriter().writeResult(("Job status: " + inst.getStatus().toString()));
    getWriter().writeResult(
        String.format("%1$-42s%2$-12s%3$-9s%4$-20s%5$s", "TaskName", "Type", "Status", "StartTime",
            "Duration"));
    for (Map.Entry<String, TaskStatus> s : status.entrySet()) {
      getWriter().writeResult(
          String.format("%1$-42s%2$-12s%3$-9s%4$-20s%5$ds", s.getKey(), s.getValue().getType(), s
              .getValue().getStatus().toString(), "", ""));
    }
  }

  public void configure(CommandLine cl) throws LogViewArgumentException {
    String[] args = cl.getArgs();
    if (args.length == 0) {
      throw new LogViewArgumentException("no instance id");
    } else if (args.length > 1) {
      throw new LogViewArgumentException("unexpected argument: " + args[1]);
    }
    ctx.setInstanceByName(args[0]);
  }

  @Override
  public String getActionName() {
    return ACTION_NAME;
  }

  public String getHelpPrefix() {
    return "log status <instance id>";
  }

}
