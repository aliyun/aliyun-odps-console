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

package com.aliyun.openservices.odps.console.stream;

import java.io.PrintStream;
import java.util.Map;
import java.util.HashMap;

import com.google.gson.*;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

import java.util.regex.*;

public class CreateStreamJobCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"galaxy", "create", "add", "stream", "streamjob"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: create streamjob <streamjobname> as <sql> end streamjob");
  }

  static private String streamJobName = "";
  static private String sql = "";
  static private boolean hit = false;

  public CreateStreamJobCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    Gson gson = new Gson();
    Map<String, String> hints = new HashMap<String, String>();
    if (QueryUtil.getTaskConfig().get("settings") != null) {
      hints = gson.fromJson(QueryUtil.getTaskConfig().get("settings"), Map.class);
    }
    String result = odps.streamJobs().create(streamJobName, sql, hints);
    outputWriter.writeResult(result);
    sql = "";
  }

  public static CreateStreamJobCommand parse(String commandString, ExecutionContext sessionContext) {
    assert (commandString != null);
    String tempString = commandString.toUpperCase();

    String params[] = commandString.trim().split("\\s+");
    String beginPattern = "\\s*CREATE\\s+STREAMJOB\\s+(.*)\\s+AS\\s+([\\d\\D]*)\\s*";
    String endPattern = "\\s*END\\s+STREAMJOB\\s*";

    //check create streamjob begin
    Pattern pattern = Pattern.compile(beginPattern);
    Matcher matcher = pattern.matcher(tempString);
    if (matcher.matches()) {
      hit = true;
      streamJobName = params[2];
      sql = commandString.trim().split("\\s+(?i)AS\\s+")[1].trim();
      return new DummyStreamJobCommand(commandString, sessionContext);
    }

    //check create streamjob end
    pattern = Pattern.compile(endPattern);
    matcher = pattern.matcher(tempString);
    if (matcher.matches()) {
      hit = false;
      sql += ";";
      return new CreateStreamJobCommand(sql, sessionContext);
    }

    //compose streamjob sql
    if (hit) {
      sql += ";" + commandString;
      return new DummyStreamJobCommand(commandString, sessionContext);
    }

    return null;
  }

}
