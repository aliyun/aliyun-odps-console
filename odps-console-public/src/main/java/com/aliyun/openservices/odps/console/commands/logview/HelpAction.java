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

package com.aliyun.openservices.odps.console.commands.logview;

import java.io.PrintStream;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class HelpAction extends LogViewBaseAction {

  public static final String ACTION_NAME = "help";

  private String auxiliaryMessage;
  private LogViewBaseAction targetAction;

  @Override
  public void parse(String[] args) {
    if (args.length > 0) {
      targetAction = ActionRegistry.getAction(args[0], ctx);
      if (targetAction == null) {
        auxiliaryMessage = "No subcommand '" + args[0] + "'";
      }
    }
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    if (auxiliaryMessage != null) {
      getWriter().writeError(auxiliaryMessage);
      getWriter().writeError("");
    }
    if (targetAction != null) {
      // Help on a specific action
      Options options = targetAction.getOptions();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(targetAction.getHelpPrefix(), options);
      return;
    }
    // Overall help message
    getWriter().writeError("usage:");
    getWriter().writeError(" log <subcommand> [options] [args]");
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String key : ActionRegistry.getActionNames()) {
      // Hide status action temporarily
      if (key.equalsIgnoreCase(GetStatusAction.ACTION_NAME)) {
        continue;
      }
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(key);
    }
    getWriter().writeError("\nAvailable subcommands are: " + sb.toString());
  }

  public static void printHelpInfo(PrintStream stream) {
    // Overall help message
    stream.println("Usage: log <subcommand> [options] [args]");
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String key : ActionRegistry.getActionNames()) {
      // Hide status action temporarily
      if (key.equalsIgnoreCase(GetStatusAction.ACTION_NAME)) {
        continue;
      }
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(key);
    }
    stream.println("Available subcommands are: " + sb.toString());
    stream.println("Use 'log help <subcommand>' to get more info");
  }

  @Override
  public String getActionName() {
    return ACTION_NAME;
  }

  public LogViewBaseAction getTargetAction() {
    return targetAction;
  }

  public void setTargetAction(LogViewBaseAction action) {
    targetAction = action;
  }

  public String getAuxiliaryMessage() {
    return auxiliaryMessage;
  }

  public void setAuxiliaryMessage(String auxiliaryMessage) {
    this.auxiliaryMessage = auxiliaryMessage;
  }

  @Override
  public String getHelpPrefix() {
    return "log help [subcommand]";
  }

}
