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

import static com.aliyun.openservices.odps.console.commands.SetCommand.SQL_DEFAULT_SCHEMA;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLHandshakeException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.odps.Tenant;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class UseProjectCommand extends DirectCommand {

  private static final String OPTION_PROJECT_NAME = "--project";

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: use <project name>;");
    stream.println("Notice: this command will clear all session settings");
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*USE\\s+(\\w+)\\s*",
      Pattern.CASE_INSENSITIVE);

  private final String projectName;

  public UseProjectCommand(
      String commandText,
      ExecutionContext context,
      String projectName) {
    super(commandText, context);
    this.projectName = projectName;
  }

  @Override
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
          String msg = "WARNING: untrusted https connection:'" + getContext().getEndpoint() + "', add https_check=true in config file to avoid this warning.";
          getContext().getOutputWriter().writeError(msg);
          odps.getRestClient().setIgnoreCerts(true);
          project = odps.projects().get(projectName);
          project.reload();
        }
      } else {
        throw e;
      }
    }

    clearSession();
    initSession(odps, project);
    getContext().setInitialized(true);

    if (getContext().isInteractiveMode()) {
      getContext().print();
    }
  }

  private void clearSession() {
    // Flags
    SetCommand.aliasMap.clear();
    SetCommand.setMap.clear();
    // Timezone
    getContext().setSqlTimezone(TimeZone.getDefault().getID());
    // Quota
    getContext().setQuotaName(null);
    getContext().setQuotaRegionId(null);
    // Interactive session
    if (getContext().isInteractiveQuery()) {
      getContext().getOutputWriter().writeError(
          "You are under interactive mode, use another project will exit interactive mode.");
      getContext().getOutputWriter().writeError("Exiting...");
      // clear session context
      try {
        if (ExecutionContext.getExecutor().isActive()) {
          ExecutionContext.getExecutor().getInstance().stop();
        }
        ExecutionContext.setExecutor(null);
        getContext().setInteractiveQuery(false);
        getContext().getOutputWriter().writeError("You are in offline mode now.");
      } catch (Exception e) {
        getContext().getOutputWriter().writeErrorFormat(
            "Exception happened when exiting interactive mode, message: %s",
            e.getMessage());
      }
    }
  }

  private void initSession(Odps odps, Project project) throws OdpsException, ODPSConsoleException {
    // User agent
    SetCommand.setMap.put("odps.idata.useragent", ODPSConsoleUtils.getUserAgent());
    // Timezone and schemaFlag
    try {
      Map<String, String> projectProps = project.getAllProperties();
      if (projectProps != null) {
        String tz = projectProps.get(SetCommand.SQL_TIMEZONE_FLAG);
        getContext().setSqlTimezone(tz);
        getContext().setSchemaName(null);

        Tenant tenant = odps.tenant();
        boolean parseFlag = Boolean.parseBoolean(tenant.getProperty(ODPSConsoleConstants.ODPS_NAMESPACE_SCHEMA));
        getContext().setOdpsNamespaceSchema(parseFlag);
      }
    } catch (Exception | NoSuchMethodError e) {
      getContext().getOutputWriter().writeDebug(e);
    }
    // Predefined settings
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
    // Project and schema
    getContext().setProjectName(projectName);
    // Priority
    getContext().setPriority(ExecutionContext.DEFAULT_PRIORITY);
    getContext().setPaiPriority(ExecutionContext.DEFAULT_PAI_PRIORITY);
  }

  /**
   * 通过传递的参数，解析出对应的command
   */
  public static UseProjectCommand parse(List<String> optionList, ExecutionContext sessionContext) {
    String projectName = ODPSConsoleUtils.shiftOption(optionList, OPTION_PROJECT_NAME);
    if (!StringUtils.isNullOrEmpty(projectName)) {
      return new UseProjectCommand("", sessionContext, projectName);
    }
    return null;
  }

  public static UseProjectCommand parse(String commandString, ExecutionContext sessionContext) {
    Matcher matcher = PATTERN.matcher(commandString);
    if (matcher.matches()) {
      return new UseProjectCommand(
          commandString,
          sessionContext,
          matcher.group(1));
    }
    return null;
  }
}
