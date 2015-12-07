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

import com.aliyun.odps.Odps;
import com.aliyun.odps.StreamJobs;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class PauseStreamJobCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"galaxy", "pause", "stream", "streamjob"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: pause streamjob <streamjobname>");
  }

  private String streamJobName;

  public PauseStreamJobCommand(String commandText, ExecutionContext context, String streamJobName) {
    super(commandText, context);
    this.streamJobName = streamJobName;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    Odps odps = getCurrentOdps();
     
    StreamJobs streamJobs = odps.streamJobs();
    String result = streamJobs.pause(streamJobName);
    outputWriter.writeResult(result);
  }  
   
  public static PauseStreamJobCommand parse(String commandString, ExecutionContext sessionContext) {
    if (commandString == null) {
        return null;
    }
    String tempString = commandString.toUpperCase();
        
    String streamJobName;
    String params[] = commandString.trim().split("\\s+");
    String streamJobStatusPattern = "\\s*PAUSE\\s+STREAMJOB\\s+.*";
    if (tempString.matches(streamJobStatusPattern) && (params.length == 3))
    {
        return new PauseStreamJobCommand(commandString, sessionContext, params[2]);
    }
    return null;
  }
}
