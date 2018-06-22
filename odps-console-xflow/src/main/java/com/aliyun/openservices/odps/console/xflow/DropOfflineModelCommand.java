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

package com.aliyun.openservices.odps.console.xflow;

import java.io.PrintStream;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

public class DropOfflineModelCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"drop", "delete", "kill", "offline", "model", "offlinemodel"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: drop offlinemodel [if exists] <offlinemodel_name>");
  }

  private String modelName;
  private boolean ifExists;
  
  public DropOfflineModelCommand(String modelName, String commandString, ExecutionContext sessionContext) {
    super(commandString, sessionContext);
    this.modelName = modelName;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    if (isIfExists()) {
      if (!getCurrentOdps().offlineModels().exists(modelName)) {
        System.out.println("OK");
        return;
      }
    }
    getCurrentOdps().offlineModels().delete(modelName);
    System.out.println("OK");
  }

  public static DropOfflineModelCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    boolean ifExists = false;
    
    // 检查是否符合DROP　OFFLINEMODEL MODEL_NAME命令
    if (commandString.toUpperCase().matches("\\s*DROP\\s+OFFLINEMODEL\\s+.*")) {
      String line = commandString.trim().replaceAll("\\s+", " ");
      line = line.substring("DROP OFFLINEMODEL ".length()).trim();
      if (line.toUpperCase().startsWith("IF EXISTS ")) {
        ifExists = true;
        line = line.substring("IF EXISTS ".length()).trim();
      }
      
      String temp[] = line.split(" ");
      if (temp.length == 1) {
        DropOfflineModelCommand command =  new DropOfflineModelCommand(temp[0], commandString, sessionContext);
        command.setIfExists(ifExists);
        return command;
      } else
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }
    return null;
  }

  private boolean isIfExists() {
    return ifExists;
  }

  private void setIfExists(boolean ifExists) {
    this.ifExists = ifExists;
  }
}
