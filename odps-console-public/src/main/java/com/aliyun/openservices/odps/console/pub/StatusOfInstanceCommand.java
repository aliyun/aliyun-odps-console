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

package com.aliyun.openservices.odps.console.pub;

import java.io.PrintStream;
import java.util.Map;

import org.apache.commons.lang.WordUtils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.common.InstanceContext;
import com.aliyun.openservices.odps.console.common.JobDetailInfo;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ConnectionCreator;

public class StatusOfInstanceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"status", "instance"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: status [extended] <instanceID>");
  }

  private String instanceId;
  private boolean extended;

  private InstanceContext instanceContext;

  public StatusOfInstanceCommand(String instanceId, String commandText, ExecutionContext context,
                                 boolean extended, InstanceContext instanceContext) {
    super(commandText, context);
    this.instanceId = instanceId;
    this.extended = extended;
    this.instanceContext = instanceContext;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();

    if (extended == false) {
      Instance inst = odps.instances().get(instanceId);
      Map<String, TaskStatus> taskStatus = inst.getTaskStatus();
      for (TaskStatus task : taskStatus.values()) {
        String status = task.getStatus().toString();
        status = WordUtils.capitalizeFully(status);
        // 打印出status, 打出task的执行状态
        getWriter().writeResult(status);
      }
    } else {
      JobDetailInfo jobDetailInfo = new JobDetailInfo(instanceContext);
      jobDetailInfo.printJobDetails();
    }
  }

  public static StatusOfInstanceCommand parse(String command, ExecutionContext session)
      throws ODPSConsoleException, OdpsException {

    if (command.toUpperCase().matches("\\s*STATUS\\s+EXTENDED\\s+.*")) {

      command = command.trim();
      String str[] = command.split("\\s+");
      if (str.length != 3)
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);

      InstanceContext instanceContext = new InstanceContext();
      if (instanceContext.getConn() == null) {
        ConnectionCreator tmpCC = new ConnectionCreator();
        instanceContext.setConn(tmpCC.create(session));
      }
      instanceContext.setSession(session);
      instanceContext.setProjectByName(session.getProjectName());

      instanceContext.setInstanceById(str[2]);
      StatusOfInstanceCommand comm = new StatusOfInstanceCommand(str[2], command, session, true, instanceContext);
      return comm;

    } else if (command.toUpperCase().matches("\\s*STATUS\\s+.*")) {
      command = command.trim();
      String str[] = command.split("\\s+");
      if (str.length == 3)
        return null;
      if (str.length != 2)
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      return new StatusOfInstanceCommand(str[1], command, session, false, null);
    }
    return null;
  }
}
