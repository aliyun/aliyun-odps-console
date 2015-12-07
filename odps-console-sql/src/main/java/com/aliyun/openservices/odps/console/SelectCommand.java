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

import java.io.PrintStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map.Entry;

import org.json.JSONObject;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.task.SqlPlanTask;
import com.aliyun.openservices.odps.console.commands.MultiClusterCommandBase;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

import jline.console.UserInterruptException;

/**
 * 支持select语句
 * 
 * @author shuman.gansm
 * */
public class SelectCommand extends MultiClusterCommandBase {

  public static final String[] HELP_TAGS = new String[]{"select", "sql"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: select <selectstatement>");
  }

  public SelectCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public void run() throws OdpsException, ODPSConsoleException {

    try {
      if (getContext().isAsyncMode()) {
        getWriter().writeError("[asysc mode]: can't support select command.");
        return;
      }

      String project = getCurrentProject();
      boolean isDryRun = getContext().isDryRun();
       if (isDryRun) {
       String taskName = "console_select_sqlplan_task_" +
       Calendar.getInstance().getTimeInMillis();
       Task dryRunTask = new SqlPlanTask(taskName, getCommandText());
      
       runJob(dryRunTask);
      
       // dry run直接返回
       return;
       }

      String taskName = "console_select_query_task_" + Calendar.getInstance().getTimeInMillis();
      SQLTask task = new SQLTask();
      task.setName(taskName);
      task.setQuery(getCommandText());

      HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();
      if (!getContext().isMachineReadable()) {
        addSetting(taskConfig, "odps.sql.select.output.format", "HumanReadable");
      }

      for (Entry<String, String> property : taskConfig.entrySet()) {
        task.setProperty(property.getKey(), property.getValue());
      }

      runJob(task);

    } catch (UserInterruptException e) {
      throw e;
    } catch (Exception e) {
      getWriter().writeDebug(e.getMessage(), e);
      throw new ODPSConsoleException(e.getMessage());
    }

  }

  private static void addSetting(HashMap<String, String> taskConfig, String key, String value) {
    String origSettings = null;
    String addedSettings = null;

    Entry<String, String> property = null;
    for (Entry<String, String> pr : taskConfig.entrySet()) {
      if (pr.getKey().equals("settings")) {
        property = pr;
        origSettings = pr.getValue();
        break;
      }
    }

    if (property == null || origSettings == null) {
      try {
        JSONObject js = new JSONObject();
        js.put(key, value);
        addedSettings = js.toString();
      } catch (Exception e) {
        return;
      }

      if (addedSettings != null) {
        taskConfig.put("settings", addedSettings);
      } else {
        return;
      }
    } else {
      try {
        JSONObject jsob = new JSONObject(origSettings);
        jsob.put(key, value);
        addedSettings = jsob.toString();
      } catch (Exception e) {
        return;
      }

      if (addedSettings != null) {
        property.setValue(addedSettings);
      } else {
        return;
      }
    }
  }

  public static SelectCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    String readCommandString = commandString;

    // 可能会有字符"\n"
    if (readCommandString.toUpperCase().matches("^SELECT[\\s\\S]*")) {

      if (!readCommandString.endsWith(";")) {
        readCommandString = readCommandString + ";";
      }

      return new SelectCommand(readCommandString, sessionContext);
    }

    return null;
  }

}
