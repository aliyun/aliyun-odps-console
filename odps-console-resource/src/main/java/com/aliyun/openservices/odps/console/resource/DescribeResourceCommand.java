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
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.CommandWithOptionP;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class DescribeResourceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"describe", "desc", "resource"};

  public static void printUsage(PrintStream stream, ExecutionContext ctx) {
    // deprecated usage
    // desc resource <resource name> -p <project name>
    // desc resource <project name>:<schema name>:<resource name>
    stream.println("Usage: ");
    if (ctx.isProjectMode()) {
      stream.println("  describe|desc resource <project name>:<resource name>");
    } else {
      stream.println("  describe|desc resource <schema name>:<resource name>");
      stream.println("  describe|desc resource <project name>:<schema name>:<resource name>");
    }
  }

  private Coordinate coordinate;

  public DescribeResourceCommand(
      Coordinate coordinate,
      String cmd,
      ExecutionContext ctx) {
    super(cmd, ctx);
    this.coordinate = coordinate;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String projectName = coordinate.getProjectName();
    String schemaName = coordinate.getSchemaName();
    String resourceName = coordinate.getObjectName();

    Odps odps = getCurrentOdps();
    if (!(odps.resources().exists(projectName, schemaName, resourceName))) {
      throw new ODPSConsoleException("Resource not found : " + resourceName);
    }
    Resource r = odps.resources().get(projectName, schemaName, resourceName);

    // TODO: outputs a frame like desc table command
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
    out.printf("%-40s%-40s\n", "LastModifiedTime",
               ODPSConsoleUtils.formatDate(r.getLastModifiedTime()));
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

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*(DESCRIBE|DESC)\\s+RESOURCE\\s+(.*)", Pattern.CASE_INSENSITIVE);

  public static DescribeResourceCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {

    Matcher m = PATTERN.matcher(cmd);
    if (!m.matches()) {
      return null;
    }

    Coordinate coordinate = parseOptionP(cmd, ctx);
    if (coordinate == null) {
      coordinate = Coordinate.getCoordinateABC(m.group(2), ":");
    }

    return new DescribeResourceCommand(coordinate, cmd, ctx);
  }

  public static Coordinate parseOptionP(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {
    CommandWithOptionP command = new CommandWithOptionP(cmd);
    if (!command.hasOptionP()) {
      return null;
    }

    // desc resource name
    if (command.getArgs().length != 3) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Invalid resource name.");
    }

    String resource = command.getArgs()[2];
    if (resource.contains(":")) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " project name conflict");
    }
    String project = command.getProjectValue();

    return Coordinate.getCoordinateOptionP(project, resource);
  }
}
