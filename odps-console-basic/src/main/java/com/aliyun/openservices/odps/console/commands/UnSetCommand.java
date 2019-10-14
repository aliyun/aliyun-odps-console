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

package com.aliyun.openservices.odps.console.commands;

import java.io.PrintStream;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

/**
 * Created by zhenhong on 15/8/31.
 */
public class UnSetCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"unset", "unalias"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: unset|unalias <key>");
  }

  boolean isSet = true;
  String key;

  public UnSetCommand(boolean isSet, String key, String commandText,
                      ExecutionContext context) {
    super(commandText, context);
    this.isSet = isSet;
    this.key = key;
  }

  public static UnSetCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    if (StringUtils.isNullOrEmpty(commandString)) {
      return null;
    }

    String[] tokens = new AntlrObject(commandString).getTokenStringArray();

    if (tokens.length != 2) {
      return null;
    }

    UnSetCommand command = null;

    if ("UNSET".equalsIgnoreCase(tokens[0])) {

      String key = tokens[1];

      command = new UnSetCommand(true, key.trim(), commandString, sessionContext);

    } else if ("UNALIAS".equalsIgnoreCase(tokens[0])) {

      String key = tokens[1];

      command = new UnSetCommand(false, key.trim(), commandString, sessionContext);
    }

    return command;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    boolean isFound = false;

    if (isSet) {
      if (SetCommand.setMap.containsKey(key)) {
        SetCommand.setMap.remove(key);
        isFound = true;
      }

      if (key.equalsIgnoreCase("odps.instance.priority")) {
        getContext().setPriority(ExecutionContext.DEFAULT_PRIORITY);
        getContext().setPaiPriority(ExecutionContext.DEFAULT_PAI_PRIORITY);
        isFound = true;
      }

      if (key.equalsIgnoreCase("odps.running.cluster")) {
        getContext().setRunningCluster(null);
        isFound = true;
      }
    } else {
      // 不能够传递
      if (SetCommand.aliasMap.containsKey(key)) {
        SetCommand.aliasMap.remove(key);
        isFound = true;
      }
    }

    if (isFound) {
      getWriter().writeError("OK");
    } else {
      getWriter().writeError(key + " not found.");
    }
  }
}
