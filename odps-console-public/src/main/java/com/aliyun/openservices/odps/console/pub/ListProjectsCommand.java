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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.ProjectFilter;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ListProjectsCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"list", "ls", "show", "project", "projects"};

  private static final String USER_TAG = "user";
  private static final String OWNER_TAG = "owner";

  private static final Options opts = new Options();

  static {
    Option userName = new Option(USER_TAG, true, "project user");
    Option ownerName = new Option(OWNER_TAG, true, "projet owner");

    userName.setRequired(false);
    ownerName.setRequired(false);

    opts.addOption(userName);
    opts.addOption(ownerName);
  }

  private ProjectFilter filter = null;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: list projects [-user <user_account>] [-owner <owner_account>]");
  }

  public ListProjectsCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public void setProjectFilter(ProjectFilter filter) {
    this.filter = filter;
  }

  static CommandLine getCommandLine(String[] args) throws ODPSConsoleException {
    if (args == null || args.length < 1) {
      throw new ODPSConsoleException("Invalid parameters - Generic options must be specified.");
    }

    CommandLineParser clp = new GnuParser();
    CommandLine cl;
    try {
      cl = clp.parse(opts, args, false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }

    return cl;
  }

  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    Iterator<Project> projects = odps.projects().iteratorByFilter(filter);
    //check permission
    projects.hasNext();

    String projectTitle[] = { "Project Name", "Comment", "Creation Time", "Last Modified Time", "Owner" };
    // 设置每一列的百分比
    int columnPercent[] = { 20, 20, 20, 20, 20};
    int consoleWidth = getContext().getConsoleWidth();

    ODPSConsoleUtils.formaterTableRow(projectTitle, columnPercent, consoleWidth);

    long size = 0;
    for (; projects.hasNext();) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Project p = projects.next();
      String projectAttr[] = new String[5];
      projectAttr[0] = p.getName();
      projectAttr[1] = p.getComment() == null ? " " : p.getComment();
      projectAttr[2] = p.getCreatedTime() == null ? " " : ODPSConsoleUtils.formatDate(p
          .getCreatedTime());
      projectAttr[3] = p.getLastModifiedTime() == null ? " " : ODPSConsoleUtils.formatDate(p
          .getLastModifiedTime());
      projectAttr[4] = p.getOwner();

      ++size;
      ODPSConsoleUtils.formaterTableRow(projectAttr, columnPercent, consoleWidth);
    }

    getWriter().writeError(size + " projects");
  }

  private static final Pattern LIST_PATTERN = Pattern.compile(
      "\\s*LIST\\s+PROJECTS($|\\s+([\\s\\S]*))", Pattern.CASE_INSENSITIVE);

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static ListProjectsCommand parse(String commandString, ExecutionContext context)
      throws ODPSConsoleException {
    Matcher matcher = LIST_PATTERN.matcher(commandString);
    if (matcher.matches()) {
      String params = matcher.group(matcher.groupCount());

      ListProjectsCommand command = new ListProjectsCommand(commandString, context);
      if (params == null || params.trim().isEmpty()) {
        return command;
      }
      CommandLine commandLine = getCommandLine(new AntlrObject(params).getTokenStringArray());

      if (commandLine.hasOption(USER_TAG) && commandLine.hasOption(OWNER_TAG)) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "The owner and user parameter should not appear together.");
      }

      if (commandLine.getArgs().length > 0) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "Invalid parameter: " + params);
      }

      ProjectFilter filter = new ProjectFilter();

      if (commandLine.hasOption(USER_TAG)) {
        filter.setUser(commandLine.getOptionValue(USER_TAG));
      } else if (commandLine.hasOption(OWNER_TAG)) {
        filter.setOwner(commandLine.getOptionValue(OWNER_TAG));
      }

      command.setProjectFilter(filter);

      return command;
    }

    return null;
  }

}
