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

import java.util.List;
import java.util.Map;

import java.util.Map.Entry;
import javax.net.ssl.SSLHandshakeException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class UseProjectCommand extends DirectCommand {

  private String projectName;
  private String msg;

  public String getProjectName() {
    return projectName;
  }

  public UseProjectCommand(String projectName, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.projectName = projectName;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = OdpsConnectionFactory.createOdps(getContext()).clone();
    odps.getRestClient().setRetryTimes(0);
    odps.getRestClient().setReadTimeout(30);

    Project project = odps.projects().get(projectName);

    odps.getRestClient().setIgnoreCerts(false);
    try {
      project.reload();
    } catch (RuntimeException e) {
      if (e.getCause() instanceof SSLHandshakeException) {
        if (getContext().isHttpsCheck()) {
          throw e;
        } else {
          msg = "WARNING: untrusted https connection:'" + getContext().getEndpoint() + "', add https_check=true in config file to avoid this warning.";
          System.err.println(msg);
          odps.getRestClient().setIgnoreCerts(true);
          project = odps.projects().get(projectName);
          project.reload();
        }
      } else {
        throw e;
      }
    }

    try {
      Map<String, String> projectProps = project.getAllProperties();
      if (projectProps != null) {
        String tz = projectProps.get(SetCommand.SQL_TIMEZONE_FLAG);
        if (!StringUtils.isNullOrEmpty(tz) && getContext().isInteractiveMode()) {
          getContext().getOutputWriter().writeError("Project timezone: " + tz);
        }
      }
    } catch (Exception e) {
      getContext().getOutputWriter().writeDebug(e);
    } catch (java.lang.NoSuchMethodError error) {
      //#ODPS-66940
      getContext().getOutputWriter().writeDebug(error);
    }
    // clear session
    SetCommand.aliasMap.clear();
    SetCommand.setMap.clear();
    // set user agent
    SetCommand.setMap.put("odps.idata.useragent", ODPSConsoleUtils.getUserAgent());

    // load predefined set commands
    Map<String, String> predefinedSetCommands = getContext().getPredefinedSetCommands();
    if (!predefinedSetCommands.isEmpty()) {
      for (Entry<String, String> entry : predefinedSetCommands.entrySet()) {
        String commandText = "SET " + entry.getKey() + "=" + entry.getValue();
        System.err.println("Executing predefined SET command: " + commandText);
        SetCommand setCommand = new SetCommand(true, entry.getKey(), entry.getValue(),
            commandText, getContext());
        setCommand.run();
      }
    }
    if (getContext().isInteractiveQuery()) {
      getContext().getOutputWriter().writeError(
          "You are under interactive mode, use another project will exit interactive mode.");
      getContext().getOutputWriter().writeError("Exiting...");
      // clear session context
      try {
        ExecutionContext.setExecutor(null);
        getContext().setInteractiveQuery(false);
      } catch (Exception ex) {
        //ignore
      }
      getContext().getOutputWriter().writeError("You are in offline mode now.");
    }
    getContext().setProjectName(projectName);
    // 默认设置为9
    getContext().setPriority(9);
    getContext().setPaiPriority(1);
  }

  /**
   * 通过传递的参数，解析出对应的command
   */
  public static UseProjectCommand parse(List<String> optionList, ExecutionContext sessionContext) {

    UseProjectCommand command = null;

    if (optionList.contains("--project")) {

      if (optionList.indexOf("--project") + 1 < optionList.size()) {

        int index = optionList.indexOf("--project");
        // 创建相应的command列表
        String cmd = optionList.get(index + 1);

        // 消费掉-e 及参数
        optionList.remove(optionList.indexOf("--project"));
        optionList.remove(optionList.indexOf(cmd));

        return new UseProjectCommand(cmd, "--project=" + cmd, sessionContext);
      }
    }

    return command;
  }

  public static UseProjectCommand parse(String commandString, ExecutionContext sessionContext) {

    if (commandString.toUpperCase().matches("\\s*USE\\s+\\w+\\s*")) {
      String temp[] = commandString.replaceAll("\\s+", " ").split(" ");
      return new UseProjectCommand(temp[1], commandString, sessionContext);
    }
    return null;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }
}
