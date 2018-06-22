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

package com.aliyun.openservices.odps.console.xflow;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.XFlows;
import com.aliyun.odps.XFlows.XResult;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class DescXFlowInstanceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"describe", "desc", "instance"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: describe|desc instance <instanceID>");
  }

  private static final Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+INSTANCE(\\s+(.*))?",
                                                         Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private final String id;
  private final String projectName;

  private DescXFlowInstanceCommand(String id, String project, String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.id = id;
    this.projectName = project;
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption("p", "project", true, "user spec project");
    return options;
  }

  private static CommandLine getCommandLine(String[] args) throws ODPSConsoleException {
    try {
      GnuParser parser = new GnuParser();
      return parser.parse(getOptions(), args);
    } catch (ParseException e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " " + e.getMessage(), e);
    }
  }

  /*
  * 注意,这个命令的的优先级高于类DescribeInstanceCommand,因此当desc某个非xflow instance时,该对象返回null,
  * 框架会继续执行, 进入DescribeInstanceCommand, 执行desc 普通instance的逻辑
  *
   */
  public static DescXFlowInstanceCommand parse(String cmd, ExecutionContext ctx) throws ODPSConsoleException {
    Matcher m = PATTERN.matcher(cmd);
    if (m.matches()) {
      String input = m.group(3);
      if (input == null) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " need specify a Instance Id.");
      }

      String[] inputs = new AntlrObject(input).getTokenStringArray();
      CommandLine commandLine = getCommandLine(inputs);

      String projectName = null;

      if (commandLine.hasOption("p")) {
        projectName = commandLine.getOptionValue("p");
      }

      if (commandLine.getArgList().size() != 1) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Invalid Instance Id.");
      }

      String instanceId = commandLine.getArgs()[0];

      if (projectName == null) {
        projectName = ctx.getProjectName();
      }

      Odps odps = OdpsConnectionFactory.createOdps(ctx);
      XFlows xFlows = odps.xFlows();
      boolean isXFlowInstance = xFlows.isXFlowInstance(odps.instances().get(projectName, instanceId));
      if (isXFlowInstance) {
        return new DescXFlowInstanceCommand(instanceId, projectName, cmd, ctx);
      } else {
        return null;
      }
    }
    return null;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = getContext();
    Odps odps = OdpsConnectionFactory.createOdps(ctx);
    Instance xInstance = odps.instances().get(projectName, id);

    if (!xInstance.isTerminated()) {
      ctx.getOutputWriter().writeError("Warning:xflow instance " + id + "is still running.");
    }

    ctx.getOutputWriter().writeResult(String.format("%-60s%-40s", "ID", xInstance.getId()));
    ctx.getOutputWriter().writeResult(String.format("%-60s%-40s", "Owner", xInstance.getOwner()));
    ctx.getOutputWriter().writeResult(String.format("%-60s%-40s",
            "StartTime", ODPSConsoleUtils.formatDate(xInstance.getStartTime())));
    if (xInstance.getEndTime() != null) {
      ctx.getOutputWriter().writeResult(String.format("%-60s%-40s",
              "EndTime", ODPSConsoleUtils.formatDate(xInstance.getEndTime())));
    }
    ctx.getOutputWriter().writeResult(
            String.format("%-60s%-40s\n", "Status", xInstance.getStatus()));

    // get xinstance's subinstances
    ArrayList xflowResults = new ArrayList();
    getSubInstanceDesc(odps, xInstance, xflowResults);

    if (!xflowResults.isEmpty()) {
      int columnPercent[] = {40, 10, 10, 10};
      String title[] = {"SubInstanceId", "TaskType", "TaskName", "TaskStatus"};
      ODPSConsoleUtils.formaterTableRow(title, columnPercent, ctx.getConsoleWidth());
      ODPSConsoleUtils.formaterTable(xflowResults, columnPercent, ctx.getConsoleWidth());
    }
  }

  private void getSubInstanceDesc(Odps odps, Instance xInstance, List<String []> subInstanceResult) throws OdpsException, ODPSConsoleException{
    for (XResult result : odps.xFlows().getXResults(xInstance).values()) {
      if ("SubWorkflow".equalsIgnoreCase(result.getNodeType())) {
        getSubInstanceDesc(odps, odps.instances().get(result.getInstanceId()), subInstanceResult);
      } else if (!"Local".equalsIgnoreCase(result.getNodeType())) {
        Instance subInstance = odps.instances().get(result.getInstanceId());
        Map<String, Instance.TaskStatus> statusMap = subInstance.getTaskStatus();
        for (Map.Entry<String, Instance.TaskStatus> entry : statusMap.entrySet()) {
          Instance.TaskStatus task = entry.getValue();

          String taskResult[] = new String[4];
          taskResult[0] = result.getInstanceId();
          taskResult[1] = task.getType();
          taskResult[2] = task.getName();
          taskResult[3] = task.getStatus().toString();

          subInstanceResult.add(taskResult);
        }
      }
    }
  }
}
