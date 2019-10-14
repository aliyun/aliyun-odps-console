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

package com.aliyun.openservices.odps.console.pub;

import java.io.PrintStream;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

import org.jline.reader.UserInterruptException;

public class StopInstanceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"kill", "stop", "instance"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: kill [instanceID]");
  }

  String instanceId;

  boolean synchronizeFlag;//true=>synchronized

  public String getInstanceId() {
    return instanceId;
  }

  public StopInstanceCommand(String commandText, ExecutionContext context, String instanceId,
                             boolean synchronizeFlag) {
    super(commandText, context);
    this.instanceId = instanceId;
    this.synchronizeFlag = synchronizeFlag;
  }

  static Options initOptions() {
    Options opts = new Options();
    Option synchronizeFlag = new Option("sync", true, "synchronizeFlag");
    synchronizeFlag.setRequired(false);

    opts.addOption(synchronizeFlag);

    return opts;
  }

  static CommandLine getCommandLine(String[] commandText) throws ODPSConsoleException {
    Options opts = initOptions();
    CommandLineParser clp = new GnuParser();
    CommandLine cl;
    try {
      cl = clp.parse(opts, commandText, false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }

    return cl;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();
    Instance instance = odps.instances().get(instanceId);
    instance.stop();

    if (synchronizeFlag) {
      while (!instance.isTerminated()) {
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          throw new UserInterruptException(e.getMessage());
        }
      }

      synchronizeFlag = false;
      writeStatus(instance.getStatus().toString());

      return;
    }

    writeStatus(null);

  }

  private void writeStatus(String status) {
    getWriter().writeError("OK");

    if (status == null) {
      getWriter().writeError("please check instance status. [status " + instanceId + ";]");
    } else {
      getWriter().writeError("instance " + instanceId + " has been " + status + " ;");
    }
  }

  public static StopInstanceCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    if (commandString.toUpperCase().startsWith("KILL")) {

      String temp[] = commandString.trim().replaceAll("\\s+", " ").split(" ");

      CommandLine cl = getCommandLine(temp);

      if (2 == cl.getArgs().length) {
        return new StopInstanceCommand(commandString, sessionContext, temp[1], false);
      }

      if (1 != cl.getArgs().length) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[invalid parameters]");
      }

      if (cl.hasOption("sync")) {
        return new StopInstanceCommand(commandString, sessionContext, cl.getOptionValue("sync"),
                                       true);
      }
    }

    return null;
  }
}
