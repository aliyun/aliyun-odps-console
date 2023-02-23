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

package com.aliyun.openservices.odps.console;


import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.text.DateFormat;


import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.openservices.odps.console.commands.SetCommand;
import org.jline.reader.UserInterruptException;

import org.jline.reader.UserInterruptException;

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ResultSet;
import com.aliyun.odps.data.SimpleStruct;


import com.aliyun.odps.task.SQLCostTask;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.task.SqlPlanTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.commands.MultiClusterCommandBase;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.output.InstanceRunner;
import com.aliyun.openservices.odps.console.utils.FormatUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * 提交匿名job，执行query
 *
 * @author shuman.gansm
 */
public class QueryCommand extends MultiClusterCommandBase {

  static final String PMC_TASK_NAME = "console_pmc_task";

  private static final String PMC_TASK_CONSOLE_KEY = "odps.console.progressive.long.running.task";

  private String taskName = "";

  private boolean isSelectCommand = false;

  private Double getSQLInputSizeInGB() {
    try {
      String queryResult = runSQLCostTask();

      JsonObject node = new JsonParser().parse(queryResult).getAsJsonObject();
      if (!node.has("Cost")
          || !node.get("Cost").getAsJsonObject().has("SQL")
          || !node.get("Cost").getAsJsonObject().get("SQL").getAsJsonObject().has("Input")) {
        return null;
      }

      Double input = node.get("Cost")
          .getAsJsonObject().get("SQL")
          .getAsJsonObject().get("Input")
          .getAsDouble();
      if (input != null) {
        return input / 1024 / 1024 / 1024;
      }
    } catch (Exception e) {
      // ignore
    }

    return null;
  }

  private String runSQLCostTask() throws OdpsException, ODPSConsoleException {
    String taskName = "console_cost_query_task_" + Calendar.getInstance().getTimeInMillis();
    SQLCostTask task = new SQLCostTask();
    task.setName(taskName);
    task.setQuery(getCommandText());

    HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();

    for (Entry<String, String> property : taskConfig.entrySet()) {
      task.setProperty(property.getKey(), property.getValue());
    }

    Instance instance = getCurrentOdps().instances().create(task);
    instance.waitForSuccess();

    return instance.getTaskResults().get(task.getName());
  }

  private String getConfirmMessage() {
    return String.format(
        "WARNING! Input data > %.3fGB, might be slow or expensive, proceed anyway (yes/no)?",
        getContext().getConfirmDataSize());
  }

  private boolean isConfirm() throws ODPSConsoleException {
    String prompt = getConfirmMessage();
    String inputStr;

    while (true) {
      inputStr = ODPSConsoleUtils.getOdpsConsoleReader().readConfirmation(prompt);

      if (inputStr == null) {
        return false;
      }

      inputStr = inputStr.trim().toUpperCase();
      if ("N".equals(inputStr) || "NO".equals(inputStr)) {
        return false;
      } else if ("Y".equals(inputStr) || "YES".equals(inputStr)) {
        return true;
      }
    }
  }

  private boolean confirmSQLInput() {
    Double threshold = getContext().getConfirmDataSize();
    if (threshold != null) {
      try {
        Double input = getSQLInputSizeInGB();

        if (input != null && input > threshold) {
          return isConfirm();
        }

      } catch (ODPSConsoleException e) {
        //ignore
      }
    }
    return true;
  }

  protected String getTaskName(boolean isDryRun) {
    if (getContext().isPMCMode()) {
      return PMC_TASK_NAME;
    }

    if (isDryRun) {
      return "console_sqlplan_task_" + Calendar.getInstance().getTimeInMillis();
    } else {
      return "console_query_task_" + Calendar.getInstance().getTimeInMillis();
    }
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = getContext();

    if ("true".equalsIgnoreCase(SetCommand.setMap.getOrDefault(PMC_TASK_CONSOLE_KEY, "false"))) {
      context.setPMCMode(true);
    } else {
      context.setPMCMode(false);
    }

    // PMCTask always accompanied by triggerandwait command, thus async mode is supported by select
    if ((!getContext().isPMCMode()) && (isSelectCommand && context.isAsyncMode())) {
      getWriter().writeError("[async mode]: can't support select command.");
      return;
    }

    if (context.isInteractiveMode()) {
      //check input confirm, if no return directly.
      if (!confirmSQLInput()) {
        return;
      }
    }

    boolean isDryRun = getContext().isDryRun();

    DefaultOutputWriter writer = context.getOutputWriter();

    // try retry
    int retryTime = isSelectCommand ? 1 : context.getRetryTimes();
    retryTime = retryTime > 0 ? retryTime : 1;
    while (retryTime > 0) {

      Task task = null;
      try {
        taskName = getTaskName(isDryRun);
        if (isDryRun) {
          task = new SqlPlanTask(taskName, getCommandText());
        } else {
          task = new SQLTask();
          task.setName(taskName);
          ((SQLTask) task).setQuery(getCommandText());
        }

        HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();
        if (!getContext().isMachineReadable()) {
          Map<String, String> settings = new HashMap<String, String>();
          settings.put("odps.sql.select.output.format", "HumanReadable");
          addSetting(taskConfig, settings);
        }

        for (Entry<String, String> property : taskConfig.entrySet()) {
          task.setProperty(property.getKey(), property.getValue());
        }

        runJob(task);

        // success
        break;
      } catch (UserInterruptException e) {
        throw e;
      } catch (Exception e) {
        // 如果是insert动态分区的sql，且判断是否能够重试
        if (task instanceof SQLTask && QueryUtil.isOperatorDisabled(((SQLTask) task).getQuery())) {
          String errorMessage = e.getMessage();

          // 两种情况不能够重试，
          // 第一明确是ODPS-0110999，
          // 第二种不包含ODPS-的未知异常，如excutor crash掉了，未知的网络问题
          if (errorMessage.contains("ODPS-0110999")) {
            // 如果是异常是ODPS-0110999不允许重试
            throw new ODPSConsoleException(e.getMessage());
          } else if (!errorMessage.contains("ODPS-")) {
            // 如果不错误包含ODPS-语句，这是未知的异常
            throw new ODPSConsoleException("ODPS-0110999:" + e.getMessage());
          }
        }

        retryTime--;
        if (retryTime == 0) {
          throw new ODPSConsoleException(e.getMessage(), e);
        }

        writer.writeError("retry " + retryTime);
        writer.writeDebug(StringUtils.stringifyException(e));
      }
    }

    // 如果返回是空的,且不是 select 语句则打出OK
    if (!isSelectCommand) {
      writer.writeError("OK");
    }
  }

  @Override
  protected void reportResult(InstanceRunner runner) throws OdpsException, ODPSConsoleException {
    super.reportResult(runner);
    if (isSelectCommand && getContext().isUseInstanceTunnel()) {
      // frames and title take 4 lines
      Long sessionMaxRow = getContext().getInstanceTunnelMaxRecord();
      String summary;
      if (sessionMaxRow != null) {
        summary = String.format(
            "A total of %d records fetched by instance tunnel. Max record number: %d",
            ((FormatUtils.FormattedResultSet) runner.getResult()).getRecordCount(),
            sessionMaxRow);
      } else {
        summary = String.format(
            "A total of %d records fetched by instance tunnel. Max record number: unlimited",
            ((FormatUtils.FormattedResultSet) runner.getResult()).getRecordCount());
      }
      getContext().getOutputWriter().writeError(summary);
    }
  }

  public String getTaskName() {
    return taskName;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public boolean isSelectCommand() {
    return isSelectCommand;
  }

  public QueryCommand(boolean isSelect, String commandText, ExecutionContext context) {
    super(commandText, context);
    isSelectCommand = isSelect;
  }

  /**
   * 通过传递的参数，解析出对应的command
   */
  public static QueryCommand parse(String commandString, ExecutionContext sessionContext) {

    boolean isSelect = false;
    commandString = commandString.trim();

    if (commandString.toUpperCase().matches("^SELECT[\\s\\S]*")) {
      isSelect = true;
    }

    if (!commandString.endsWith(";")) {
      commandString = commandString + ";";
    }
    return new QueryCommand(isSelect, commandString, sessionContext);
  }
}
