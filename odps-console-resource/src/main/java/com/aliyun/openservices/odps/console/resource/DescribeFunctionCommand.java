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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Function;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class DescribeFunctionCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"describe", "desc", "function"};

  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+FUNCTION\\s+(.*)",
                                                   Pattern.CASE_INSENSITIVE);

  public static void printUsage(PrintStream stream, ExecutionContext ctx) {
    stream.println("Usage:");
    if (ctx.isProjectMode()) {
      stream.println("  describe|desc function [<project name>.]<function name>;");
      stream.println("Example:");
      stream.println("  desc function my_function;");
      stream.println("  desc function my_project.my_function");
    } else {
      stream.println("  describe|desc function [[<project name>.]<schema name>.]<function name>;");
      stream.println("Example:");
      stream.println("  desc function my_function;");
      stream.println("  desc function my_schema.my_function");
      stream.println("  desc function my_project.my_schema.my_function");
    }
  }

  private Coordinate coordinate;

  public DescribeFunctionCommand(
      Coordinate coordinate,
      String cmd,
      ExecutionContext context) {
    super(cmd, context);
    this.coordinate = coordinate;
  }

  public static AbstractCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {
    Matcher m = PATTERN.matcher(cmd);
    if (!m.matches()) {
      return null;
    }

    Coordinate coordinate = Coordinate.getCoordinateABC(m.group(2));
    return new DescribeFunctionCommand(coordinate, cmd, ctx);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String projectName = coordinate.getProjectName();
    String schemaName = coordinate.getSchemaName();
    String functionName = coordinate.getObjectName();

    Function function;
    function = getCurrentOdps().functions().get(projectName, schemaName, functionName);

    // TODO: outputs a frame like desc table command
    System.out.println(String.format("%-40s%-40s", "Name", functionName));
    System.out.println(String.format("%-40s%-40s", "Owner", function.getOwner()));
    System.out.println(String.format("%-40s%-40s", "Created Time", ODPSConsoleUtils.formatDate(function.getCreatedTime())));

    if (function.isSqlFunction()) {
      System.out.println(String.format("%-40s%-40s", "SQL Definition Text", function.getSqlDefinitionText()));
    } else {
      System.out.println(String.format("%-40s%-40s", "Class", function.getClassPath()));
      StringBuilder builder = new StringBuilder();
      for (String name : function.getResourceFullNames()) {
        if (builder.length() != 0) {
          builder.append(",");
        }
        builder.append(name);
      }

      System.out.println(String.format("%-40s%-40s", "Resources", builder.toString()));
    }
  }
}
