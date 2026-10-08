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

package com.aliyun.openservices.odps.console.utils.antlr;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsole;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.CommandSplitter;
import com.aliyun.openservices.odps.console.utils.LogUtil;
import com.aliyun.openservices.odps.console.utils.RawStringCommandSplitter;

import java.util.List;


/**
 * This class is used in two steps in console:
 * 1. splitCommands cmds into separate commands
 * plz call splitCommands()
 * 2. parse one command and return tokens
 * plz call getTokenStringArray() or getTokenStringArrayWithParenMerged()
 *
 * because antlr's parser can only scan once,
 * so user should just choose one of the two functions above.
 */
public class AntlrObject {

  private String rawCommand;
  private CommandSplitter splitter;
  private RawStringCommandSplitter rawStringCommandSplitter;

  public AntlrObject(String rawCommand) {
    this.rawCommand = rawCommand;
    splitter = new CommandSplitter(rawCommand);
    rawStringCommandSplitter = new RawStringCommandSplitter(rawCommand);
  }

  /**
   * Parse one command
   *
   * @throws ODPSConsoleException
   */
  public String[] getTokenStringArray() throws ODPSConsoleException {
    try {
      return splitter.getTokens().toArray(new String[]{});
    } catch (ODPSConsoleException e) {
      try {
        return rawStringCommandSplitter.getTokens().toArray(new String[0]);
      } catch (Exception e1) {
        throw e;
      }
    }
  }

  public String[] getRawTokenStringArray() throws ODPSConsoleException {
    return rawStringCommandSplitter.getTokens().toArray(new String[0]);
  }

  /**
   * Split commands into command list:
   * 1. remove comments
   * 2. split by semicolon except those in STRING tokens
   * 3. reconstruct each command to it's original form
   */
  public List<String> splitCommands() throws ODPSConsoleException {
    return splitter.getCommands();
  }

  public List<String> splitCommands(ExecutionContext ctx) throws ODPSConsoleException {
    if (!ctx.isSupportRawString()) {
      return splitCommands();
    }

    List<String> commands = null;
    ODPSConsoleException e0 = null;
    try {
      commands = splitter.getCommands();
    } catch (ODPSConsoleException e1) {
      e0 = e1;
    }

    try {
      List<String> rawCommands = rawStringCommandSplitter.getCommands();

      if (rawStringCommandSplitter.getFindRawString()) {
        return rawCommands;
      } else {
        if (e0 != null) {
          throw e0;
        }
        return commands;
      }

    } catch (Exception e) {
      LogUtil.sendFallbackLog(ctx, rawCommand, "split commands, find raw string: " + rawCommand, e);
      if (e0 == null) {
        return commands;
      } else {
        throw e0;
      }
    }

  }
}
