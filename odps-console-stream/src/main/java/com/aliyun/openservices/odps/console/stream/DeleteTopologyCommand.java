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
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class DeleteTopologyCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"drop", "delete", "topology"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: drop topology <topologyname>");
  }

  private String topologyName;
  public DeleteTopologyCommand(String commandText, ExecutionContext context, String topologyName) {
    super(commandText, context);
    this.topologyName = topologyName;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();
    odps.topologies().delete(this.topologyName);

  }

  public static DeleteTopologyCommand parse(String commandString, ExecutionContext sessionContext) {
    assert (commandString != null);
    String tempString = commandString.toUpperCase();

    String streamName;
    String params[] = commandString.trim().split("\\s+");
    String deleteTopologyPattern = "\\s*DROP\\s+TOPOLOGY\\s+.*";
    if (tempString.matches(deleteTopologyPattern) && (params.length == 3))
    {
        return new DeleteTopologyCommand(commandString, sessionContext, params[2]);
    }
    return null;
  }
}
