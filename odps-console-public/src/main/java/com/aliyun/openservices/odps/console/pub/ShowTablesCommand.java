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
 * @author <a
 *         href="shenggong.wang ">shenggong.wang 
 *         </a>
 */
public class ShowTablesCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "list", "ls", "table", "tables"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: show tables [in <projectname>]");
    stream.println("       list|ls tables [-p,-project <projectname>]");
  }

  private String project;

  static Options initOptions() {
    Options opts = new Options();
    Option project_name = new Option("p", "project", true, "project name");
    project_name.setRequired(false);

    opts.addOption(project_name);

    return opts;
  }

  static CommandLine getCommandLine(String commandText) throws ODPSConsoleException {
    String[] args = ODPSConsoleUtils.translateCommandline(commandText);
    if (args == null || args.length < 2) {
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

  public ShowTablesCommand(String cmd, ExecutionContext cxt, String project) {
    super(cmd, cxt);

    this.project = project;
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

    Iterator<Table> it = odps.tables().iterator(project);

    writer.writeResult("");// for HiveUT

    while (it.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Table table = it.next();
      writer.writeResult(table.getOwner() + ":" + table.getName());
    }

    writer.writeError("\nOK");
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*SHOW\\s+TABLES(\\s*|(\\s+IN\\s+(\\w+)))\\s*", Pattern.CASE_INSENSITIVE);

  private static final Pattern PUBLIC_PATTERN = Pattern.compile(
      "\\s*(LS|LIST)\\s+TABLES.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  // for chain
  public static ShowTablesCommand parse(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {
    if (cmd == null || cxt == null) {
      return null;
    }

    String projectName = null;
    Matcher m = matchInternalCmd(cmd);

    if (m.matches()) {
      projectName = getProjectName(m);
    } else {
      m = matchPublicCmd(cmd);
      if (m.matches()) {
        projectName = getProjectNameFromPublicCmd(cmd);
      } else {
        return null;
      }
    }

    return new ShowTablesCommand(cmd, cxt, projectName);
  }

  // -- package ---
  static Matcher matchInternalCmd(String cmd) {
    return (PATTERN.matcher(cmd));
  }

  static Matcher matchPublicCmd(String cmd) {
    return PUBLIC_PATTERN.matcher(cmd);
  }

  static String getProjectName(Matcher m) {
    String projectName = null;

    if (m.matches() && m.groupCount() == 3) {
      projectName = m.group(3);
    }

    return projectName;
  }

  static String getProjectNameFromPublicCmd(String cmd) throws ODPSConsoleException {
    String projectName = null;

    CommandLine cl = getCommandLine(cmd);
    if (cl.getArgs().length > 2) {
      // Any args except 'ls' and 'tables' should be errors
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }

    if (!cl.hasOption("project")) {
      return null;
    }

    projectName = cl.getOptionValue("project");

    return projectName;
  }

}
