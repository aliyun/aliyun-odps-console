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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLCostTask;
import com.aliyun.odps.commons.util.CostResultParser;
import com.aliyun.openservices.odps.console.commands.MultiClusterCommandBase;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

public class SQLCostCommand extends MultiClusterCommandBase {

  public static final String[] HELP_TAGS = new String[]{"cost", "sql"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: cost sql <sqlstatement>");
  }

  public SQLCostCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  private static final Pattern
      PATTERN =
      Pattern.compile("^COST\\s+SQL(\\s+([\\s\\S]*))?", Pattern.CASE_INSENSITIVE);

  public void run() throws OdpsException, ODPSConsoleException {
    String query = getCommandText();
    Matcher matcher = PATTERN.matcher(query);
    if (!matcher.matches() || matcher.groupCount() != 2) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }

    query = matcher.group(2);

    String taskName = "console_cost_query_task_" + Calendar.getInstance().getTimeInMillis();
    SQLCostTask task = new SQLCostTask();
    task.setName(taskName);
    task.setQuery(query);

    HashMap<String, String> taskConfig = QueryUtil.getTaskConfig();

    for (Entry<String, String> property : taskConfig.entrySet()) {
      task.setProperty(property.getKey(), property.getValue());
    }

    runJob(task);
  }

  @Override
  public void writeResult(String queryResult) throws ODPSConsoleException {
    DefaultOutputWriter outputWriter = getContext().getOutputWriter();
    queryResult = CostResultParser.parse(queryResult, "SQL");
    outputWriter.writeResult(queryResult);
  }

  public static SQLCostCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    String readCommandString = commandString;

    // 可能会有字符"\n"
    if (PATTERN.matcher(readCommandString).matches()) {

      if (!readCommandString.endsWith(";")) {
        readCommandString = readCommandString + ";";
      }

      return new SQLCostCommand(readCommandString, sessionContext);
    }

    return null;
  }

}
