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

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

import jline.console.UserInterruptException;

/**
 * @author fengyin.zym mainly copied from QueryCommand, just keep the style
 * */
public class MergeCommand extends MultiClusterCommandBase {

  public static final String[] HELP_TAGS = new String[]{"merge", "table", "alter", "smallfile", "smallfiles"};
  private static final String MERGE_CROSSPATH_FLAG = "odps.merge.cross.paths";


  public static void printUsage(PrintStream stream) {
    stream.println("Usage: alter table <tablename> merge smallfiles");
  }

  private String taskName = "";

  public void run() throws OdpsException, ODPSConsoleException {

    ExecutionContext context = getContext();
    DefaultOutputWriter writer = context.getOutputWriter();

    // do retry
    int retryTime = context.getRetryTimes();
    retryTime = retryTime > 0 ? retryTime : 1;
    while (retryTime > 0) {
      Task task = null;
      try {
        taskName = "console_merge_task_" + Calendar.getInstance().getTimeInMillis();
        task = new MergeTask(taskName, getCommandText());

        HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();
        if (!SetCommand.setMap.containsKey(MERGE_CROSSPATH_FLAG)) {
          Map<String, String> settings = new HashMap<String, String>();
          settings.put(MERGE_CROSSPATH_FLAG, "true");
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

  public MergeCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public static MergeCommand parse(String commandString, ExecutionContext sessionContext) {
    String content = commandString;
    String regstr = "\\s*ALTER\\s+TABLE\\s+(.*)(MERGE\\s+SMALLFILES\\s*)$";

    Pattern p = Pattern.compile(regstr, Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(content);

    if (m.find()) {
      // System.out.println("tpinfo : " + m.group(1));
      // extract the table/partition info
      return new MergeCommand(m.group(1), sessionContext);
    }
    return null;
  }
}
