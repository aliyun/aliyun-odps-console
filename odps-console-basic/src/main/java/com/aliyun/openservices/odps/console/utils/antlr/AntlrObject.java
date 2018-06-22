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

import java.util.List;

import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.CommandSplitter;


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

  private CommandSplitter splitter;

  public AntlrObject(String rawCommand) {
    splitter = new CommandSplitter(rawCommand);
  }

  /**
   * Parse one command
   *
   * @throws ODPSConsoleException
   */
  public String[] getTokenStringArray() throws ODPSConsoleException {
    return splitter.getTokens().toArray(new String[]{});
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
}
