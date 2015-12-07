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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.StreamJob;
import com.aliyun.odps.StreamJobs;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class ListStreamJobCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"galaxy", "list", "stream", "streamjob", "streamjobs"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: list streamjobs");
  }

  private static final String[] GLOBAL = {"Name", "Status", "StartTime", 
    "Owner", "WorkerNum", "CpuBind"};
  public ListStreamJobCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public static String getFormat(int[] Len) {
    StringBuilder globalFormat = new StringBuilder("");
    for (int i = 0; i < Len.length; ++i) {                                                                                        
      if (i != Len.length - 1) {
        globalFormat.append("%-").append(Len[i]).append("s\t");
      } else {
        globalFormat.append("%-").append(Len[i]).append("s\n");
      }
    }
    return globalFormat.toString();
  }

  private int[] getColumnLength(StreamJobs jobs) {
    int[] len = new int[]{4, 6, 9, 5, 9, 7};
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    for (StreamJob job : jobs) {
      String[] data = new String[len.length];
      data[0] = job.getName();
      data[1] = job.getStatus();
      data[2] = df.format(job.getCreateTime());
      data[3] = job.getOwner();                                                                                          
      data[4] = String.valueOf(job.getWorkerNum());
      data[5] = String.valueOf(job.getCPUBind());
      for (int i = 0; i < len.length; ++i) {
        len[i] = data[i].length() > len[i] ?
          data[i].length() : len[i];
      }
    }
    return len;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    Odps odps = getCurrentOdps();
     
    StreamJobs streamJobs = odps.streamJobs();
    String result = getScreenDisplay(streamJobs);
    outputWriter.writeResult(result);
  }  

  private String getScreenDisplay(StreamJobs jobs) {
    String globalFormat = getFormat(getColumnLength(jobs));
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    StringWriter out = new StringWriter();
    PrintWriter w = new PrintWriter(out);
    w.printf(globalFormat, GLOBAL[0], GLOBAL[1], GLOBAL[2], GLOBAL[3], GLOBAL[4], GLOBAL[5]);
    for (StreamJob job : jobs) {
      w.printf(globalFormat, job.getName(), job.getStatus(), df.format(job.getCreateTime()), job.getOwner(),
          job.getWorkerNum(), job.getCPUBind());
    }
    w.flush();
    w.close();

    return out.toString();
  }
   
  public static ListStreamJobCommand parse(String commandString, ExecutionContext sessionContext) {
    if (commandString == null) {
        return null;
    }
    String tempString = commandString.toUpperCase();
        
    String streamName;
    String params[] = commandString.trim().split("\\s+");
    String getStreamPattern = "\\s*LIST\\s+STREAMJOBS\\s*";
    if (tempString.matches(getStreamPattern) && (params.length == 2))
    {
        return new ListStreamJobCommand(commandString, sessionContext);
    }
    return null;
  }
}
