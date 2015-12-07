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
import java.util.Arrays;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.logview.ActionRegistry;
import com.aliyun.openservices.odps.console.commands.logview.HelpAction;
import com.aliyun.openservices.odps.console.commands.logview.LogViewArgumentException;
import com.aliyun.openservices.odps.console.commands.logview.LogViewBaseAction;
import com.aliyun.openservices.odps.console.commands.logview.LogViewContext;
import com.aliyun.openservices.odps.console.utils.ConnectionCreator;

public class LogViewCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"log", "logview"};

  public static void printUsage(PrintStream stream) {
    HelpAction.printHelpInfo(stream);
  }

  public static final String COMMAND_NAME = "log";
  private static ThreadLocal<LogViewContext> context = new ThreadLocal<LogViewContext>() {
    @Override
    protected LogViewContext initialValue() {
      return new LogViewContext();
    }
  };

  public LogViewContext getLogViewContext() {
    return context.get();
  }

  public LogViewCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  private LogViewBaseAction action;

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    action.run();
  }

  public static LogViewCommand parse(String commandText, ExecutionContext session)
      throws ODPSConsoleException, OdpsException {
    String[] tokens = commandText.split("\\s+");
    commandText = commandText.trim().toLowerCase();
    if (!isLogViewCommand(tokens)) {
      return null;
    }
    // Initialize log view context
    LogViewContext ctx = context.get();
    if (ctx.getConn() == null) {
      ConnectionCreator tmpCC = new ConnectionCreator();
      ctx.setConn(tmpCC.create(session));
    }
    ctx.setSession(session);
    ctx.setProjectByName(session.getProjectName());

    // Create log view command
    LogViewCommand comm = new LogViewCommand(commandText, session);

    if (tokens.length < 2) {
      comm.action = ActionRegistry.getHelpAction(ctx, "No subcommand specified");
      return comm;
    } else {
      comm.action = ActionRegistry.getAction(tokens[1], ctx);
      if (comm.action == null) {
        // Wrong action name, print help message in this case.
        comm.action = ActionRegistry.getHelpAction(ctx, "Subcommand '" + tokens[1]
            + "' does not exists");
        return comm;
      }
    }

    // Initialize action and try to parse remaining args.
    try {
      if (tokens.length > 2) {
        comm.action.parse(Arrays.copyOfRange(tokens, 2, tokens.length));
      } else {
        comm.action.parse(new String[] {});
      }
    } catch (LogViewArgumentException e) {
      // parsing action failed, print it's help message
      HelpAction action = ActionRegistry.getHelpAction(ctx, e.getMessage());
      action.setTargetAction(comm.action);
      comm.action = action;
    }
    return comm;
  }

  private static boolean isLogViewCommand(String[] args) {
    if (args.length < 1) {
      return false;
    }
    return args[0].toLowerCase().equals(COMMAND_NAME);
  }

}
