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

package com.aliyun.openservices.odps.console.xflow;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.XFlow;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class ShowXflowsCommand extends AbstractCommand {

  public static final String[] HELP_TAGS =
      new String[]{"show", "xflows", "xflow"};

  private String projectName = null;
  private String publicProject = "algo_public";
  private String xflowOwner = null;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: show|list xflows [-p,-project <project_name>] owner");
  }

  public ShowXflowsCommand(String projectName, String xflowOwner, String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
    this.projectName = projectName;
    this.xflowOwner = xflowOwner;
  }

  public void ShowXflows(String projectName) throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    long size = 0;
    Iterator<XFlow> xflows = odps.xFlows().iterator(projectName, xflowOwner);
    getWriter().writeError("");
    while(xflows.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();
      XFlow xflow = xflows.next();
      getWriter().writeResult(xflow.getOwner() + ":" + xflow.getName());
      ++size;
    }
    getWriter().writeError(size + " xflows in project " + projectName + ".\n");
    System.out.flush();
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    // if option projectName not specifed
    // show xflows in current project and project algo_public
    if (projectName == null) {
      String curProject = this.getContext().getProjectName();
      ShowXflows(curProject);
      if (!curProject.equals(publicProject)) {
        ShowXflows(publicProject);
      }
    } else {
      // show xflows in specified projectName
      ShowXflows(projectName);
    }
    getWriter().writeError("OK");
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption("p", "project", true, "user spec project");
    return options;
  }

  private static CommandLine getCommandLine(String[] args) throws ODPSConsoleException {
    try {
      GnuParser parser = new GnuParser();
      return parser.parse(getOptions(), args);
    } catch (ParseException e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " " + e.getMessage(), e);
    }
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*(SHOW|LIST)\\s+XFLOWS($|\\s+(.*))", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static ShowXflowsCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }
    Matcher matcher = PATTERN.matcher(cmd);

    if (!matcher.matches()) {
      return null;
    }

    String input = matcher.group(2);
    String[] inputs = new AntlrObject(input).getTokenStringArray();
    CommandLine commandLine = getCommandLine(inputs);

    String projectName = null;

    if (commandLine.hasOption("p")) {
      projectName = commandLine.getOptionValue("p");
    }

    if (commandLine.getArgList().size() > 1) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid command.");
    }

    String owner = null;
    if (commandLine.getArgList().size() == 1) {
      owner = commandLine.getArgs()[0];
    }

    ShowXflowsCommand command = new ShowXflowsCommand(projectName, owner, cmd, ctx);
    return command;
  }
}
