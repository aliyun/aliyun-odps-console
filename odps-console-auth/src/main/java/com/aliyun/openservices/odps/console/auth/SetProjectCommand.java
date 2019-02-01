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

package com.aliyun.openservices.odps.console.auth;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

public class SetProjectCommand extends AbstractCommand {

  private String commandText;

  public static final String[] HELP_TAGS = new String[]{"setproject", "set", "project"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: setproject <key>=<value>");
  }

  public SetProjectCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
    this.commandText = commandText;

  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    try {
      Odps odps = getCurrentOdps();
      Project project = odps.projects().get();
      Map<String, String> properties;

      if (commandText.isEmpty()) {
        properties = project.getAllProperties();

        if (properties != null) {
          // print all properties
          for (Map.Entry<String, String> property : properties.entrySet()) {
            getWriter().writeError(property.getKey() + "=" + property.getValue());
          }
        }
        getWriter().writeError("OK");
        return;
      }

      properties = project.getProperties();

      if (properties == null) {
        properties = new HashMap<String, String>();
      }

      String[] args = commandText.split("=");
      if (args.length == 1) {
        // just has one entry, remove the property
        properties.put(args[0].trim(), "");
      } else if (args.length == 2) {
        properties.put(args[0].trim(), args[1].trim());
      } else {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }

      odps.projects().updateProject(getCurrentProject(), properties);

      getWriter().writeError("OK");
    } catch (OdpsException e) {
      throw new ODPSConsoleException(e.getMessage(), e);
    }
  }

  public static SetProjectCommand parse(String commandString, ExecutionContext context) {

    if (commandString.toUpperCase().matches("^SETPROJECT\\s*.*")) {
      return new SetProjectCommand(commandString.substring(10).trim(), context);
    }
    return null;
  }

}
