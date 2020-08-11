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

package com.aliyun.openservices.odps.console.commands;

import java.io.PrintStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jline.reader.UserInterruptException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

/**
 * @author zhenhong.gzh
 * **/
public class CompactCommand extends MultiClusterCommandBase {

  public static final String[]
      HELP_TAGS =
      new String[]{"merge", "table", "alter", "compact"};


  private String compactType;

  public static void printUsage(PrintStream stream) {
    stream.println(
        "Usage: alter table <table_name> [partition (partition_key = 'partition_value' [, ...])] compact [major|minor]");
  }

  private String taskName = "";

  private boolean checkTransactional(String tablePart) throws OdpsException, ODPSConsoleException {
    String projectName = getCurrentProject();
    String tableName = tablePart.split("\\s+")[0];

    if (tableName.contains(".")) {
      tableName = tableName.split("\\.")[1];
    }

    return getCurrentOdps().tables().get(projectName, tableName).isTransactional();
  }

  public void run() throws OdpsException, ODPSConsoleException {

    ExecutionContext context = getContext();
    DefaultOutputWriter writer = context.getOutputWriter();

    if (!checkTransactional(getCommandText())) {
      throw new OdpsException(getCommandText() +  " is not a transactional table.");
    }

    // do retry
    int retryTime = context.getRetryTimes();
    retryTime = retryTime > 0 ? retryTime : 1;
    while (retryTime > 0) {
      Task task = null;
      try {
        taskName = "console_merge_task_" + Calendar.getInstance().getTimeInMillis();
        task = new MergeTask(taskName, getCommandText());

        HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();

        Map<String, String> settings = new HashMap<String, String>();
        settings.put("odps.merge.txn.table.compact", compactType);
        settings.put("odps.merge.restructure.action", "hardlink");
        addSetting(taskConfig, settings);

        for (Entry<String, String> property : taskConfig.entrySet()) {
          task.setProperty(property.getKey(), property.getValue());
        }

        runJob(task);
        // success
        break;
      } catch (UserInterruptException e) {
        throw e;
      } catch (Exception e) {
        retryTime--;
        if (retryTime == 0) {
          throw new ODPSConsoleException(e.getMessage());
        }
        writer.writeError("retry " + retryTime);
        writer.writeDebug(StringUtils.stringifyException(e));
      }
    }
    // no exception ,print success
    writer.writeError("OK");
  }

  public String getTaskName() {
    return taskName;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public CompactCommand(String command, String compactType, ExecutionContext context) {
    super(command, context);

    this.compactType = compactType;
  }

  private static String getCompactType(String type) {
    if (StringUtils.isNullOrEmpty(type)) {
      return null;
    }

    if (type.equalsIgnoreCase("major")) {
      return "major_compact";
    } else if (type.equalsIgnoreCase("minor")) {
      return "minor_compact";
    }

    return null;
  }

  public static CompactCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    String regstr = "\\s*ALTER\\s+TABLE\\s+(.*)\\s+COMPACT\\s+(.*)";

    Pattern p = Pattern.compile(regstr, Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(commandString);

    if (m.find()) {
      String tablePart = m.group(1).trim();
      String compactType = getCompactType(m.group(2).trim());

      if (compactType == null) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Compact type should be MAJOR or MINOR.");
      }

      return new CompactCommand(tablePart, compactType, sessionContext);
    }
    return null;
  }
}
