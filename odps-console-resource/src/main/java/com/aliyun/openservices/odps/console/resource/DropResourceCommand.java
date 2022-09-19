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
import java.util.regex.Pattern;

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

public class DropResourceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"drop", "delete", "resource"};

  public static void printUsage(PrintStream out, ExecutionContext ctx) {
    // deprecated usage
    // delete resource -p <project name>
    // todo support if exists like drop function
    if (ctx.isProjectMode()) {
      out.println("Usage: drop resource [<project name>:]<resource name>");
    } else {
      out.println("Usage: drop resource [[<project name>:]<schema name>]:<resource name>");
    }
  }

  private Coordinate coordinate;

  public DropResourceCommand(
      Coordinate coordinate,
      String commandText,
      ExecutionContext context) {
    super(commandText, context);
    this.coordinate = coordinate;
  }

  @Override
  public void run() throws ODPSConsoleException, OdpsException {
    coordinate.interpretByCtx(getContext());
    /**
     * Should be null if the project name is not specified.
     */
    String projectName = coordinate.getProjectName();
    /**
     * Should be null if the schema name is not specified.
     */
    String schemaName = coordinate.getSchemaName();
    String resourceName = coordinate.getObjectName();

    Odps odps = getCurrentOdps();

    try {
      odps.resources().delete(projectName, schemaName, resourceName);
      getWriter().writeError("OK");
    } catch (NoSuchObjectException e) {
      getWriter().writeError(e.getMessage());
    } catch (OdpsException e) {
      throw new ODPSConsoleException(e.getMessage());
    }
  }

  public static DropResourceCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    // 1. check match
    String[] args = CommandParserUtils.getCommandTokens(commandString);
    if (args.length < 3) {
      return null;
    }

    boolean matchDelete = "DELETE".equalsIgnoreCase(args[0]) && "RESOURCE".equalsIgnoreCase(args[1]);
    boolean matchDrop = "DROP".equalsIgnoreCase(args[0]) && "RESOURCE".equalsIgnoreCase(args[1]);

    if (!matchDelete && !matchDrop) {
      return null;
    }

    CommandWithOptionP cmdP = new CommandWithOptionP(commandString);
    // 2. parse
    Coordinate coordinate;
    String project = null;
    String resource = null;
    if (matchDelete) {
      project = cmdP.getProjectValue();
      args = cmdP.getArgs();
    }

    if (args.length != 3) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }
    resource = args[2];

    if (matchDelete) {
      if (resource.contains(":")) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
      coordinate = Coordinate.getCoordinateOptionP(project, resource);
    } else {
      coordinate = Coordinate.getCoordinateABC(resource, ":");
    }

    return new DropResourceCommand(coordinate, commandString, sessionContext);
  }
}
