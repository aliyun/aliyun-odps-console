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
import com.aliyun.odps.Topology;
import com.aliyun.odps.Topologies;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class ListTopologyCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"list", "topology", "topologies"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: list topologies");
  }

  public ListTopologyCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    Odps odps = getCurrentOdps();
     
    String topologyList = "Topologies: ";
    Topologies topologies = odps.topologies();
    for (Topology t : topologies) {
        topologyList += t.getName() + ", ";
    }
    outputWriter.writeResult(topologyList);
  }  
   
  public static ListTopologyCommand parse(String commandString, ExecutionContext sessionContext) {
    if (commandString == null) {
        return null;
    }
    String tempString = commandString.toUpperCase();
        
    String streamName;
    String params[] = commandString.trim().split("\\s+");
    String getStreamPattern = "\\s*LIST\\s+TOPOLOGIES\\s*";
    if (tempString.matches(getStreamPattern) && (params.length == 2))
    {
        return new ListTopologyCommand(commandString, sessionContext);
    }
    return null;
  }
}
