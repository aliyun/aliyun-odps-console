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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;

public class DropResourceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"drop", "delete", "resource"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: drop resource <resourcename>");
    out.println("       delete resource <resourcename> [-p,-project <projectname>]");
  }

  private String projectName;
  private String resourceName;

  public DropResourceCommand(String projectName, String resourceName, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.projectName = projectName;
    this.resourceName = resourceName;
  }
  
  static Options initOptions() {
    Options opts = new Options();
    Option project_name = new Option("p", "project", true, "project name");
    project_name.setRequired(false);
    opts.addOption(project_name);
    return opts;
  }

  @Override
  public void run() throws ODPSConsoleException, OdpsException {

    Odps odps = getCurrentOdps();
    try {
      if (StringUtils.isNullOrEmpty(projectName)) {
        odps.resources().delete(resourceName);
      } else {
        odps.resources().delete(projectName, resourceName);
      }

      getWriter().writeError("OK");
    } catch (NoSuchObjectException e) {
      getWriter().writeError(e.getMessage());
    } catch (OdpsException e) {
      throw new ODPSConsoleException(e.getMessage());
    }
    
  }
  
  public static DropResourceCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    String[] args = CommandParserUtils.getCommandTokens(commandString);
    if (args.length < 2) {
      return null;
    }

    // 检查是否符合DROP　RESOURCE RESOURCE_NAME命令
    if (args[0].equalsIgnoreCase("DROP") && args[1].equalsIgnoreCase("RESOURCE")) {
      if (args.length == 3) {
        return new DropResourceCommand(null, args[2], commandString, sessionContext);
      } else {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
    } else if (args[0].equalsIgnoreCase("DELETE") && args[1].equalsIgnoreCase("RESOURCE")) {
      Options opts = initOptions();
      CommandLine cl=CommandParserUtils.getCommandLine(args, opts);
      
      if (3 != cl.getArgs().length) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
      String project = cl.getOptionValue("p");
      return new DropResourceCommand(project, cl.getArgs()[2], commandString, sessionContext);
    }

    return null;
  }

}
