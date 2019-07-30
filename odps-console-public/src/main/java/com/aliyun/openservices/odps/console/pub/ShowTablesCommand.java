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

/**
 *
 */
package com.aliyun.openservices.odps.console.pub;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.TableFilter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * List tables in the project
 *
 * SHOW TABLES [IN project_name];
 *
 */
public class ShowTablesCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "list", "ls", "table", "tables"};

  private static final Pattern PATTERN = Pattern.compile(
          "\\s*SHOW\\s+TABLES(\\s*|(\\s+IN\\s+(\\w+)))(\\s*|(\\s+LIKE\\s+\'(\\w*)(\\*|%)\'))\\s*",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern PUBLIC_PATTERN = Pattern.compile(
          "\\s*(LS|LIST)\\s+TABLES.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final int PATTERN_GROUP_COUNT = 7; // indicate the group count in pattern regex

  private static final int PREFIX_GROUP_INDEX = 6;  // indicate the index of prefix in pattern regex

  private static final int PROJECT_GROUP_INDEX = 3; // indicate the index of projectname in pattern regex

  private static final int PUBLIC_CMD_ARG_COUNT = 2;  // indicate the valid arg count in public command

  private String project;

  private String prefix;

  public ShowTablesCommand(String cmd, ExecutionContext cxt, String project) {
    super(cmd, cxt);

    this.project = project;
  }

  public ShowTablesCommand(String cmd, ExecutionContext cxt, String project, String prefix) {
    this(cmd, cxt, project);

    this.prefix = prefix;
  }

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: show tables [in <project_name>] [like '<prefix>']");
    stream.println("       list|ls tables [-p,-project <project_name>]");
  }

  static Options initOptions() {
    Options opts = new Options();
    Option projectNameOpt = new Option("p", "project", true, "project name");
    projectNameOpt.setRequired(false);

    opts.addOption(projectNameOpt);

    return opts;
  }

  static CommandLine getCommandLine(String commandText) throws ODPSConsoleException {
    String[] args = ODPSConsoleUtils.translateCommandline(commandText);
    if (args == null || args.length < 2) {
      throw new ODPSConsoleException("Invalid parameters - Generic options must be specified.");
    }

    Options opts = initOptions();
    CommandLineParser commandLineParser = new GnuParser();
    CommandLine commandLine;
    try {
      commandLine = commandLineParser.parse(opts, args, false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }

    return commandLine;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.aliyun.openservices.odps.console.commands.AbstractCommand#run()
   */
  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    DefaultOutputWriter writer = getContext().getOutputWriter();

    Odps odps = OdpsConnectionFactory.createOdps(getContext());

    if (null == project) {
      project = getCurrentProject();
    }

    TableFilter prefixFilter = new TableFilter();
    prefixFilter.setName(prefix); // prefix filter, default prefix is null
    Iterator<Table> it = odps.tables().iterator(project, prefixFilter);

    writer.writeResult("");// for HiveUT

    while (it.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();
      Table table = it.next();
      writer.writeResult(table.getOwner() + ":" + table.getName());
    }

    writer.writeError("\nOK");
  }

  // for chain
  public static ShowTablesCommand parse(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {
    if (cmd == null || cxt == null) {
      return null;
    }

    String projectName = null;
    String prefixName = null;
    Matcher matcher = matchInternalCmd(cmd);

    if (matcher.matches()) {
      projectName = getProjectName(matcher);
      prefixName = getPrefixName(matcher);
    } else {
      matcher = matchPublicCmd(cmd);
      if (matcher.matches()) {
        projectName = getProjectNameFromPublicCmd(cmd);
      } else {
        return null;
      }
    }

    return new ShowTablesCommand(cmd, cxt, projectName, prefixName);
  }

  // -- package ---
  static Matcher matchInternalCmd(String cmd) {
    return (PATTERN.matcher(cmd));
  }

  static Matcher matchPublicCmd(String cmd) {
    return PUBLIC_PATTERN.matcher(cmd);
  }

  static String getProjectName(Matcher matcher) {
    String projectName = null;

    if (matcher.matches() && matcher.groupCount() == PATTERN_GROUP_COUNT) {
      // standard show tables command or show tables with prefix command
      projectName = matcher.group(PROJECT_GROUP_INDEX);
    }

    return projectName;
  }

  static String getProjectNameFromPublicCmd(String cmd) throws ODPSConsoleException {
    String projectName = null;

    CommandLine commandLine = getCommandLine(cmd);
    if (commandLine.getArgs().length > PUBLIC_CMD_ARG_COUNT) {
      // Any args except 'ls' and 'tables' should be errors
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }

    if (!commandLine.hasOption("project")) {
      return null;
    }

    projectName = commandLine.getOptionValue("project");

    return projectName;
  }

  static String getPrefixName(Matcher matcher) {
    String prefixName = null;

    if (matcher.matches() && matcher.groupCount() == PATTERN_GROUP_COUNT
            && matcher.group(PREFIX_GROUP_INDEX) != null) {
      // if the prefix is empty, just let it remain null
      prefixName = matcher.group(PREFIX_GROUP_INDEX);
    }

    return prefixName;
  }

}
