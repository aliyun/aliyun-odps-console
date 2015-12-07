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

package com.aliyun.openservices.odps.console.pub;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.Set;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.SetCommand;

/**
 * 显示set和alias设置的值
 * 
 * @author shuman.gansm
 * */
public class ShowFlagsCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "set", "alias", "flag", "flags"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: show flags");
  }

  public ShowFlagsCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws ODPSConsoleException, OdpsException {

    getWriter().writeError("Set config:");
    Set<String> setKeySet = SetCommand.setMap.keySet();
    for (Iterator<String> it = setKeySet.iterator(); it.hasNext();) {
      String key = it.next();
      getWriter().writeError(key + "=" + SetCommand.setMap.get(key));

    }

    getWriter().writeError("Alias config:");
    Set<String> aliasKeySet = SetCommand.aliasMap.keySet();
    for (Iterator<String> it = aliasKeySet.iterator(); it.hasNext();) {
      String key = it.next();
      getWriter().writeError(key + "=" + SetCommand.aliasMap.get(key));

    }

  }

  public static ShowFlagsCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    if (commandString.trim().toUpperCase().matches("SHOW\\s+FLAGS")) {

      return new ShowFlagsCommand(commandString, sessionContext);
    }
    return null;
  }

}
