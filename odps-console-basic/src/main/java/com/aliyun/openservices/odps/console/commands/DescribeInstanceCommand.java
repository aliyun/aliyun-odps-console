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

package com.aliyun.openservices.odps.console.commands;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class DescribeInstanceCommand extends AbstractCommand {

  private String projectName;
  private String instanceId;

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public DescribeInstanceCommand(String instanceId, String projectName, String cmd,
                                 ExecutionContext ctx) {
    super(cmd, ctx);
    this.instanceId = instanceId;
    this.projectName = projectName;
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

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    if (!(odps.instances().exists(projectName, instanceId))) {
      throw new ODPSConsoleException("Instance not found : " + instanceId);
    }
    Instance i = odps.instances().get(projectName, instanceId);

    PrintWriter out = new PrintWriter(System.out);

    out.printf("%-40s%-40s\n", "ID", i.getId());
    out.printf("%-40s%-40s\n", "Owner", i.getOwner());
    out.printf("%-40s%-40s\n", "StartTime", DATE_FORMAT.format(i.getStartTime()));
    if (i.getEndTime() != null) {
      out.printf("%-40s%-40s\n", "EndTime", DATE_FORMAT.format(i.getEndTime()));
    }
    out.printf("%-40s%-40s\n", "Status", i.getStatus());
    for (Map.Entry<String, Instance.TaskStatus> entry : i.getTaskStatus().entrySet()) {
      out.printf("%-40s%-40s\n", entry.getKey(), StringUtils.capitalize(entry.getValue().getStatus().toString().toLowerCase()));
    }
    for (Task task : i.getTasks()) {
      out.printf("%-40s%-40s\n", "Query", task.getCommandText());
    }

    out.flush();
  }

  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+INSTANCE(\\s+(.*))?",
                                                   Pattern.CASE_INSENSITIVE);

  public static DescribeInstanceCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }

    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    String input = m.group(3);
    if (input == null) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " need specify one Instance Id.");
    }
    String[] inputs = new AntlrObject(input).getTokenStringArray();
    CommandLine commandLine = getCommandLine(inputs);

    String projectName = null;

    if (commandLine.hasOption("p")) {
      projectName = commandLine.getOptionValue("p");
    }

    if (commandLine.getArgList().size() != 1) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Invalid Instance Id.");
    }

    String resourceName = commandLine.getArgs()[0];

    if (projectName == null) {
      projectName = ctx.getProjectName();
    }

    return new DescribeInstanceCommand(resourceName, projectName, cmd, ctx);

  }

}
