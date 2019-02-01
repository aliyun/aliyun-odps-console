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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.ExportProjectUtil;
import java.io.PrintStream;

public class ExportProjectCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"export", "project"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: export <projectname> <local_path>  [-rftpd]");
  }

  private String projectName;
  private String localPath;

  private String options;

  public String getProjectName() {
    return projectName;
  }

  public String getLocalPath() {
    return localPath;
  }

  public String getOptions() {
    return options;
  }

  public ExportProjectCommand(String commandText, ExecutionContext context, String projectName,
      String localPath, String options) {
    super(commandText, context);

    this.projectName = projectName;
    this.localPath = localPath;
    this.options = options;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();
    odps.setDefaultProject(projectName);
    // export project to local
    ExportProjectUtil.exportProject(odps, localPath, getContext(), getOptions());

  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static ExportProjectCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    if (commandString.trim().toUpperCase().startsWith("EXPORT")) {

      String temp[] = commandString.trim().split("\\s+");

      if (temp.length == 3) {
        return new ExportProjectCommand(commandString, sessionContext, temp[1], temp[2], "-rftpd");
      } else if (temp.length == 4) {
        return new ExportProjectCommand(commandString, sessionContext, temp[1], temp[2], temp[3]);
      }
      // export 命令可以给sql去执行，由sql来抛错
    }

    return null;
  }

}
