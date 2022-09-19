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

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.CommandWithOptionP;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;


public class DropFunctionCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"drop", "delete", "function"};

  public static void printUsage(PrintStream out, ExecutionContext ctx) {
    out.println("Usage: ");
    if (ctx.isProjectMode()) {
      out.println(" drop function [if exists] [<project name>.]<function name>");
    } else {
      out.println(" drop function [if exists] [[<project name>.]<schema name>.]<function name>");
    }
  }

  private Coordinate coordinate;
  /**
   * Should be null if the project name is not specified.
   */
  private String projectName;
  /**
   * Should be null if the schema name is not specified.
   */
  private String schemaName;
  private String functionName;
  // if not exists, do nothing, just return ok.
  boolean checkExistence;

  public DropFunctionCommand(
      boolean checkExistence,
      Coordinate coordinate,
      String commandText,
      ExecutionContext context) {
    super(commandText, context);
    this.coordinate = coordinate;
    this.checkExistence = checkExistence;
  }

  @Override
  public void run() throws ODPSConsoleException, OdpsException {
    coordinate.interpretByCtx(getContext());
    projectName = coordinate.getProjectName();
    schemaName = coordinate.getSchemaName();
    functionName = coordinate.getObjectName();

    Odps odps = getCurrentOdps();
    try {
      if (checkExistence && !odps.functions().exists(projectName, schemaName, functionName)) {
        getWriter().writeError("OK");
        return;
      }
      odps.functions().delete(projectName, schemaName, functionName);
      getWriter().writeError("OK");
    } catch (NoSuchObjectException e) {
      getWriter().writeError(e.getMessage());
    } catch (OdpsException e) {
      throw new ODPSConsoleException(e.getMessage());
    }
  }

  public static DropFunctionCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    // 1. check match
    String[] args = CommandParserUtils.getCommandTokens(commandString);
    if (args.length < 2) {
      return null;
    }
    boolean matchDrop = "DROP".equalsIgnoreCase(args[0]) && "FUNCTION".equalsIgnoreCase(args[1]);
    boolean matchDelete = "DELETE".equalsIgnoreCase(args[0]) && "FUNCTION".equalsIgnoreCase(args[1]);
    if (!matchDelete && !matchDrop) {
      return null;
    }

    // 2. parse
    Coordinate coordinate;
    String project = null;
    String function = null;
    boolean checkExists = false;

    CommandWithOptionP cmdP = new CommandWithOptionP(commandString);
    // get -p
    if (matchDelete) {
      project = cmdP.getProjectValue();
      args = cmdP.getArgs();
    }

    // get functionName and ifExists
    if (args.length == 3) {
      // DROP FUNCTION NAME
      function = args[2];
    } else if (args.length == 5) {
      // DROP FUNCTION IF EXISTS NAME
      boolean matchIfExists = "IF".equalsIgnoreCase(args[2]) &&
                              "EXISTS".equalsIgnoreCase(args[3]);
      if (!matchIfExists) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
      function = args[4];
      checkExists = true;
    } else {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }

    // set project.schema.function
    if (matchDelete) {
      if (function.contains(".")) {
        // DELETE CMD not support pj.schema.function grammar
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
      coordinate = Coordinate.getCoordinateOptionP(project, function);
    } else {
      coordinate = Coordinate.getCoordinateABC(function);
    }

    return new DropFunctionCommand(checkExists, coordinate, commandString, sessionContext);
  }
}
