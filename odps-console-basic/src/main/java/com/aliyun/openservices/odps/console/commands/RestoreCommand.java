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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jline.reader.UserInterruptException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.MergeTask;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

/**
 * Restore table or partition from cold storage
 */
public class RestoreCommand extends MultiClusterCommandBase {

  public static final String[] HELP_TAGS = new String[]{"restore", "cold", "storage"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: alter table <table name> (<partition spec>)? restore");
  }

  public RestoreCommand(
      String commandText,
      ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  protected void run() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = getContext();
    DefaultOutputWriter writer = context.getOutputWriter();

    // do retry
    int retryTime = context.getRetryTimes();
    retryTime = retryTime > 0 ? retryTime : 1;
    while (true) {
      try {
        MergeTask task;
        String taskName = "console_cold_storage_restore_task_"
            + Calendar.getInstance().getTimeInMillis();
        task = new MergeTask(taskName, getCommandText());

        HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();
        addSetting(taskConfig, Collections.singletonMap("odps.merge.cold.storage.mode", "restore"));

        for (Entry<String, String> property : taskConfig.entrySet()) {
          task.setProperty(property.getKey(), property.getValue());
        }

        runJob(task);
        // on success
        writer.writeError("OK");
        break;
      } catch (UserInterruptException e) {
        throw e;
      } catch (Exception e) {
        if (--retryTime <= 0) {
          throw new ODPSConsoleException(e.getMessage());
        }
        writer.writeError("retry " + retryTime);
        writer.writeDebug(StringUtils.stringifyException(e));
      }
    }
  }

  public static RestoreCommand parse(String commandString,
                                     ExecutionContext sessionContext) {
    String regex = "\\s*ALTER\\s+TABLE\\s+(.*)\\s+(RESTORE\\s*)$";

    Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    Matcher m = p.matcher(commandString);

    if (m.find()) {
      // extract the table/partition info
      return new RestoreCommand(m.group(1), sessionContext);
    }

    return null;
  }

}
