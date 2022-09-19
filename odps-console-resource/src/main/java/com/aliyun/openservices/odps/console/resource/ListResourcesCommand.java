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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.Table;
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
public class ListResourcesCommand extends AbstractCommand {

  public static final String[] HELP_TAGS =
      new String[]{"list", "ls", "show", "resource", "resources"};

  private static final String coordinateGroup = "coordinate";
  private static final Pattern PATTERN = Pattern.compile(
      "\\s*(LS|LIST|SHOW)\\s+RESOURCES(\\s+IN\\s+(?<coordinate>[\\w+.]+))?\\s*",
      Pattern.CASE_INSENSITIVE);

  public static void printUsage(PrintStream out, ExecutionContext ctx) {
    if (ctx.isProjectMode()) {
      out.println("Usage: show resources in [<project name>[.<schema name>]]");
    } else {
      out.println("Usage: show resources in [[<project name>.]<schema name>]]");
    }
  }

  private Coordinate coordinate;

  public ListResourcesCommand(Coordinate coordinate,
                              String commandText, ExecutionContext context) {
    super(commandText, context);
    this.coordinate = coordinate;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String project = coordinate.getProjectName();
    String schema = coordinate.getSchemaName();
    Odps odps = getCurrentOdps();

    int[] columnPercent = new int[]{15, 15, 15, 15, 5, 5, 10, 10, 10};
    String[] headers = new String[]{
        "Resource Name",
        "Owner",
        "Creation Time",
        "Last Modified Time",
        "Type",
        "Last Updator",
        "Resource Size",
        "Source",
        "Comment"};

    Iterator<Resource> iterator;

    iterator = odps.resources().iterator(project, schema);

    //check permission
    iterator.hasNext();

    ODPSConsoleUtils
        .formaterTableRow(headers, columnPercent, getContext().getConsoleWidth());

    int totalCount = 0;
    int columnCounter = 0;
    while (iterator.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Resource p = iterator.next();
      String[] resourceAttr = new String[columnPercent.length];
      resourceAttr[columnCounter++] = ODPSConsoleUtils.safeGetString(p, "getName");
      resourceAttr[columnCounter++] = ODPSConsoleUtils.safeGetString(p, "getOwner");
      resourceAttr[columnCounter++] = ODPSConsoleUtils.safeGetDateString(p, "getCreatedTime");
      resourceAttr[columnCounter++] = ODPSConsoleUtils.safeGetDateString(p, "getLastModifiedTime");
      resourceAttr[columnCounter++] = ODPSConsoleUtils.safeGetString(p, "getType").toLowerCase();
      resourceAttr[columnCounter++] = ODPSConsoleUtils.safeGetString(p, "getLastUpdator");
      resourceAttr[columnCounter++] = ODPSConsoleUtils.safeGetString(p, "getSize");
      if (p.getType() == Resource.Type.TABLE) {
        Table sourceTable = (Table) ODPSConsoleUtils.safeGetObject(p, "getSourceTable");
        String tableSource = sourceTable == null ?
                             " " : sourceTable.getProject() + "." + sourceTable.getName();
        resourceAttr[columnCounter++] = tableSource;
      } else {
        resourceAttr[columnCounter++] = "";
      }

      resourceAttr[columnCounter] = ODPSConsoleUtils.safeGetString(p, "getComment");

      ODPSConsoleUtils.formaterTableRow(resourceAttr, columnPercent, getContext()
          .getConsoleWidth());
      totalCount++;
      columnCounter = 0;
    }

    getWriter().writeError(totalCount + " resources");
  }

  public static ListResourcesCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    Matcher matcher = PATTERN.matcher(commandString);
    if (!matcher.matches()) {
      return null;
    }

    Coordinate coordinate = Coordinate.getCoordinateAB(matcher.group(coordinateGroup));
    return new ListResourcesCommand(coordinate, commandString, sessionContext);
  }
}
