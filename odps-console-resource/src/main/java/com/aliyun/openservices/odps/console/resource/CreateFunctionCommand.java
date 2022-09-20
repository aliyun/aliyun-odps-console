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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.aliyun.odps.Function;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;


public class CreateFunctionCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"create", "add", "function"};

  public static void printUsage(PrintStream stream, ExecutionContext ctx) {
    // using resource_name
    // using project/resources/resource_name
    // using schema/resources/resource_name
    stream.println("Usage:");
    if (ctx.isProjectMode()) {
      stream.println("  create function [<project name>.]<function name> as <full class name>"
                     + " using [<project name>/resources]<resource name>;");
      stream.println("Examples:");
      stream.println("  create function my_function as 'my.class'"
                     + " using 'my_resource_1', 'my_resource_2';");
      stream.println("  create function my_project.my_function"
                     + " as 'my.class' using 'my_project/resources/my_resource';");
    } else {
      stream.println("  create function [[<project name>.]<schema name>.]<function name>"
                     + " as <full class name>"
                     + " using [[<project name>/schemas/]<schema_name>/resources/]<resource name>;");
      stream.println("Examples:");
      stream.println("  create function my_function as 'my.class'"
                      + " using 'my_resource_1', 'my_resource_2';");
      stream.println("  create function my_project.my_schema.my_function"
                     + " as 'my.class' using 'my_schema/resources/my_resource';");
      stream.println("  create function my_project.my_schema.my_function"
                     + " as 'my.class' using 'my_project/schemas/my_schema/resources/my_resource';");
    }
  }

  private Coordinate coordinate;
  private boolean isUpdate;
  private String className;
  private List<Coordinate> resCoordinateList;

  public CreateFunctionCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public CreateFunctionCommand(Coordinate coordinate,
                               boolean isUpdate,
                               String className,
                               List<Coordinate> resCoordinateList,
                               String commandText,
                               ExecutionContext context) {
    super(commandText, context);
    this.coordinate = coordinate;
    this.isUpdate = isUpdate;
    this.className = className;
    this.resCoordinateList = resCoordinateList;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String projectName = coordinate.getProjectName();
    String schemaName = coordinate.getSchemaName();
    String functionName = coordinate.getObjectName();

    Odps odps = getCurrentOdps();
    Function function = new Function();
    function.setName(functionName);
    function.setClassPath(className);

    for (Coordinate coordinate: resCoordinateList) {
      coordinate.interpretByCtx(getContext());
    }

    List<String> resList = resCoordinateList.stream().map(coordinate1 -> {
      if (getContext().isProjectMode()) {
        if (coordinate1.getProjectName().equals(getContext().getProjectName())) {
          return coordinate1.getObjectName();
        }
        return coordinate1.getProjectName() + "/resources/" + coordinate1.getObjectName();
      } else {
        // use getdisplayschemaname get default not null
        return coordinate1.getProjectName() + "/schemas/"+ coordinate1.getDisplaySchemaName() +
               "/resources/" + coordinate1.getObjectName();
      }
    }).collect(Collectors.toList());
    function.setResources(resList);

    if (isUpdate) {
      try {
        odps.functions().update(projectName, schemaName, function);
        getWriter().writeError("Success: Function " + functionName + " have been updated.");
        return;
      } catch (NoSuchObjectException ignore) {
        // if function not exists, goto create mode
      }
    }
    odps.functions().create(projectName, schemaName, function);
    getWriter().writeError("Success: Function '" + functionName + "' have been created.");
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*CREATE\\s+FUNCTION\\s+([\\w.]+)\\s+AS\\s+(\\S+)\\s+USING\\s+(.+)",
      Pattern.CASE_INSENSITIVE);

  public static CreateFunctionCommand parse(String commandString, ExecutionContext ctx)
      throws ODPSConsoleException {

    // file/py/jar/archive
    Matcher matcher = PATTERN.matcher(commandString);
    if (!matcher.matches()) {
      return null;
    }

    String functionPart = matcher.group(1);
    Coordinate coordinate = Coordinate.getCoordinateABC(functionPart);

    String className = matcher.group(2).replaceAll("['\"\\s]", "");

    // get update
    // TODO: remove unix style
    String using = matcher.group(3).trim();
    boolean isUpdate = false;
    if (using.endsWith(" -f")) {
      using = using.substring(0, using.lastIndexOf(" -f"));
      isUpdate = true;
    }

    // get resource list
    List<String> resList = Arrays.asList(
        using.replaceAll("['\"\\s]", "").split(","));

    List<Coordinate> resCoordinateList = Coordinate.getCoordinateRes(resList);
    // String currentProject = ODPSConsoleUtils.getDefaultProject(ctx);
    // String currentSchema = ODPSConsoleUtils.getParseDisplaySchema(ctx);
    // resList = resList.stream().map(res -> {
    //   if (res.contains("/")) {
    //     if (ctx.isParseProjectMode()) {
    //       return res;
    //     } else {
    //       if (res.split("/").length < 5) {
    //         return currentProject + "/schemas/" + res;
    //       } else {
    //         return res;
    //       }
    //     }
    //   } else {
    //     if (ctx.isParseProjectMode()) {
    //       return currentProject + "/resources/" + res;
    //     } else {
    //       return currentProject + "/schemas/" + currentSchema + "/resources/" + res;
    //     }
    //   }
    // }).collect(Collectors.toList());

    return new CreateFunctionCommand(coordinate,
                                     isUpdate,
                                     className,
                                     resCoordinateList,
                                     commandString,
                                     ctx);
  }
}
