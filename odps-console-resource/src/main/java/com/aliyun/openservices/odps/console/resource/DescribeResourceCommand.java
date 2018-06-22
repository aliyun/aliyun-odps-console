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
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class DescribeResourceCommand extends AbstractCommand {

  private String projectName;
  private String resourceName;

  public static final String[] HELP_TAGS = new String[]{"describe", "desc", "resource"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: describe|desc resource [-p,-project <projectname>] <resourcename>");
    stream.println("       describe|desc resource [<projectname>.]<resourcename>");
  }

  public DescribeResourceCommand(String resourceName, String projectName, String cmd,
                                 ExecutionContext ctx) {
    super(cmd, ctx);
    this.resourceName = resourceName;
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
    if (!(odps.resources().exists(projectName, resourceName))) {
      throw new ODPSConsoleException("Resource not found : " + resourceName);
    }
    Resource r = odps.resources().get(projectName, resourceName);

    PrintWriter out = new PrintWriter(System.out);

    out.printf("%-40s%-40s\n", "Name", r.getName());
    out.printf("%-40s%-40s\n", "Owner", r.getOwner());
    out.printf("%-40s%-40s\n", "Type", r.getType());
    if (r.getType() == Resource.Type.TABLE) {
      TableResource tr = (TableResource) r;
      String tableSource = tr.getSourceTable().getProject() + "." + tr.getSourceTable().getName();
      if (tr.getSourceTablePartition() != null) {
        tableSource += " partition(" + tr.getSourceTablePartition().toString() + ")";
      }
      out.printf("%-40s%-40s\n", "SourceTableName", tableSource);
    }
    out.printf("%-40s%-40s\n", "Comment", r.getComment());
    out.printf("%-40s%-40s\n", "CreatedTime", ODPSConsoleUtils.formatDate(r.getCreatedTime()));
    out.printf("%-40s%-40s\n", "LastModifiedTime", ODPSConsoleUtils.formatDate(r.getLastModifiedTime()));
    out.printf("%-40s%-40s\n", "LastUpdator", r.getLastUpdator());

    if (r.getSize() != null) {
      out.printf("%-40s%-40s\n", "Size", r.getSize());
    }

    if ((r instanceof FileResource) &&
        !StringUtils.isNullOrEmpty(((FileResource) r).getContentMd5())) {
      out.printf("%-40s%-40s\n", "Md5sum", ((FileResource) r).getContentMd5());
    }
    out.flush();
  }

  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+RESOURCE\\s+(.*)",
                                                   Pattern.CASE_INSENSITIVE);

  public static DescribeResourceCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    if (cmd == null || ctx == null) {
      return null;
    }

    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    String input = m.group(2);
    String[] inputs = new AntlrObject(input).getTokenStringArray();
    CommandLine commandLine = getCommandLine(inputs);

    String projectName = null;

    if (commandLine.hasOption("p")) {
      projectName = commandLine.getOptionValue("p");
    }

    if (commandLine.getArgList().size() != 1) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Invalid resource name.");
    }

    String resourceName = commandLine.getArgs()[0];

    if (resourceName.contains(":")) {
      String[] result = resourceName.split(":", 2);
      if (projectName != null && (!result[0].equals(projectName))) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " project name conflict.");
      }
      projectName = result[0];
      resourceName = result[1];
    }

    if (projectName == null) {
      projectName = ctx.getProjectName();
    }

    return new DescribeResourceCommand(resourceName, projectName, cmd, ctx);

  }

}
