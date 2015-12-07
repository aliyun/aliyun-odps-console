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
import com.aliyun.odps.StreamJobs;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.QueryUtil;

public class ResumeStreamJobCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"galaxy", "resume", "stream", "streamjob"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: resume streamjob <streamjobname>");
  }

  private String streamJobName;

  public ResumeStreamJobCommand(String commandText, ExecutionContext context, String streamJobName) {
    super(commandText, context);
    this.streamJobName = streamJobName;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    Odps odps = getCurrentOdps();
     
    StreamJobs streamJobs = odps.streamJobs();
    Gson gson = new Gson();
    Map<String, String> hints = new HashMap<String, String>();
    if (QueryUtil.getTaskConfig().get("settings") != null) {
      hints = gson.fromJson(QueryUtil.getTaskConfig().get("settings"), Map.class);
    }
    String result = streamJobs.resume(streamJobName, hints);
    outputWriter.writeResult(result);
  }  
   
  public static ResumeStreamJobCommand parse(String commandString, ExecutionContext sessionContext) {
    if (commandString == null) {
        return null;
    }
    String tempString = commandString.toUpperCase();
        
    String streamJobName;
    String params[] = commandString.trim().split("\\s+");
    String streamJobStatusPattern = "\\s*RESUME\\s+STREAMJOB\\s+.*";
    if (tempString.matches(streamJobStatusPattern) && (params.length == 3))
    {
        return new ResumeStreamJobCommand(commandString, sessionContext, params[2]);
    }
    return null;
  }
}
