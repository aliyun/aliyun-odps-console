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

import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

/**
 * list resources
 *
 * @author shuman.gansm
 */
public class ListFunctionsCommand extends AbstractCommand {

  public static final String[] HELP_TAGS =
      new String[]{"list", "ls", "show", "function", "functions"};

  public static void printUsage(PrintStream out, ExecutionContext ctx) {
    if (ctx.isProjectMode()) {
      out.println("Usage: show functions [in/from <project name>[.<schema name>]] [like '<prefix>'];");
      out.println("Examples:");
      out.println("  show functions;");
      out.println("  show functions like 'my_%';");
      out.println("  show functions in my_project;");
      out.println("  show functions from my_project;");
      out.println("  show functions in my_project.my_schema;");
      out.println("  show functions from my_project.my_schema;");
    } else {
      out.println("Usage: show functions [in/from [<project name>.]<schema name>] [like '<prefix>'];");
      out.println("Examples:");
      out.println("  show functions;");
      out.println("  show functions like 'my_%';");
      out.println("  show functions in my_schema;");
      out.println("  show functions from my_schema;");
      out.println("  show functions in my_project.my_schema;");
      out.println("  show functions from my_project.my_schema;");
    }
  }

  private static final String coordinateGroup = "coordinate";
  private static final String prefixGroup = "prefix";
  private static final Pattern PATTERN = Pattern.compile(
      "\\s*(LS|LIST|SHOW)\\s+FUNCTIONS(\\s+(IN|FROM)\\s+(?<coordinate>[\\w.]+))?\\s*"
      + "(\\s*|(\\s+LIKE\\s+'(?<prefix>\\w*)(\\*|%)'))\\s*",
      // "\\s*(LS|LIST|SHOW)\\s+FUNCTIONS(\\s+IN\\s+(\\w+)(\\.(\\w+))?)?\\s*",
      Pattern.CASE_INSENSITIVE);

  private Coordinate coordinate;
  private String prefix;

  public ListFunctionsCommand(Coordinate coordinate, String commandText, ExecutionContext context,
                              String prefix) {
    super(commandText, context);
    this.coordinate = coordinate;
    this.prefix = prefix;
  }

  @Override
  public void run() throws ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String project = coordinate.getProjectName();
    String schema = coordinate.getSchemaName();

    Odps odps = getCurrentOdps();

    String[] headers = {"Name", "Owner", "Create Time", "Class", "Resources"};
    int[] columnPercent = {12, 20, 15, 23, 30};
    int consoleWidth = getContext().getConsoleWidth();

    Iterator<Function> functionIter = odps.functions().iterator(project, schema, prefix);

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

    // TODO: time taken & fetched rows
    getWriter().writeError(count + " functions");
  }

  public static ListFunctionsCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    Matcher matcher = PATTERN.matcher(commandString);
    if (!matcher.matches()) {
      return null;
    }

    Coordinate coordinate = Coordinate.getCoordinateAB(matcher.group(coordinateGroup));
    String prefixName = matcher.group(prefixGroup);
    return new ListFunctionsCommand(coordinate, commandString, sessionContext, prefixName);
  }

}
