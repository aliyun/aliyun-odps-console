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

package com.aliyun.openservices.odps.console.resource;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.Function;
import com.aliyun.odps.Functions;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

/**
 * list resources
 *
 * @author shuman.gansm
 */
public class ListFunctionsCommand extends AbstractCommand {

  public static final String[]
      HELP_TAGS =
      new String[]{"list", "ls", "show", "function", "functions"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: list functions");
    out.println("       ls functions [-p <projectname>]");
  }

  private String project;

  public ListFunctionsCommand(String commandText, ExecutionContext context, String project) {
    super(commandText, context);

    this.project = project;
  }

  static Options initOptions() {
    Options opts = new Options();
    Option project_name = new Option("p", true, "project name");

    project_name.setRequired(false);

    opts.addOption(project_name);

    return opts;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();

    String[] headers = {"Name", "Owner", "Create Time", "Class", "Resources"};
    int[] columnPercent = {12, 20, 15, 23, 30};
    int consoleWidth = getContext().getConsoleWidth();

    Iterator<Function> functionIter;

    if (project == null) {
      functionIter = odps.functions().iterator();
    } else {
      functionIter = odps.functions().iterator(project);
    }

    // Check permission before printing headers
    functionIter.hasNext();

    ODPSConsoleUtils.formaterTableRow(headers, columnPercent, consoleWidth);

    int count = 0;
    while (functionIter.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();

      count++;

      Function p = functionIter.next();
      String[] functionAttr = new String[5];
      functionAttr[0] = p.getName();
      functionAttr[1] = p.getOwner();
      functionAttr[2] =
          p.getCreatedTime() == null ? " " : ODPSConsoleUtils.formatDate(p.getCreatedTime());
      functionAttr[3] = p.getClassPath();
      functionAttr[4] = "";
      for (String name : p.getResourceNames()) {
        functionAttr[4] += name + ",";
      }

      int endIndex = functionAttr[4].lastIndexOf(",");
      if (endIndex != -1) {
        functionAttr[4] = functionAttr[4].substring(0, endIndex);
      }

      ODPSConsoleUtils.formaterTableRow(functionAttr, columnPercent, consoleWidth);
    }

    getWriter().writeError(count + " functions");
  }

  static CommandLine getCommandLine(String cmd) throws ODPSConsoleException {

    AntlrObject antlr = new AntlrObject(cmd);
    String[] args = antlr.getTokenStringArray();

    if (args == null) {
      throw new ODPSConsoleException("Invalid parameters - Generic options must be specified.");
    }

    Options opts = initOptions();
    CommandLineParser clp = new GnuParser();
    CommandLine cl;
    try {
      cl = clp.parse(opts, args, false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }

    return cl;
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*(LS|LIST)\\s+FUNCTIONS(\\s*|(\\s+([\\s\\S]*)))\\s*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static ListFunctionsCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    Matcher matcher = PATTERN.matcher(commandString);

   if (matcher.matches()) {
      String project = null;

      if (4 == matcher.groupCount() && matcher.group(4) != null) {
        CommandLine cl = getCommandLine(matcher.group(4));
        if (0 != cl.getArgs().length) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[invalid paras]");
        }
        project = cl.getOptionValue("p");
      }
      return new ListFunctionsCommand(commandString, sessionContext, project);
    }

    return null;
  }

}
