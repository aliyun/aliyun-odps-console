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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityConfiguration;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.FileUtil;

/**
 * set\alias命令
 * 
 * @author shuman.gansm
 * */
public class SetCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"set", "alias"};
  public static final String SQL_TIMEZONE_FLAG = "odps.sql.timezone";

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: set|alias <key>=<value>");
  }

  // session map
  public static Map<String, String> setMap = new HashMap<String, String>();
  public static Map<String, String> aliasMap = new HashMap<String, String>();

  private static List<String> aclList = Arrays.asList("OBJECTCREATORHASACCESSPERMISSION",
      "OBJECTCREATORHASGRANTPERMISSION", "CHECKPERMISSIONUSINGACL", "CHECKPERMISSIONUSINGPOLICY",
      "PROJECTPROTECTION", "LABELSECURITY");

  boolean isSet = true;
  String key;
  String value;

  public boolean isSet() {
    return isSet;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public SetCommand(boolean isSet, String key, String value, String commandText,
      ExecutionContext context) {
    super(commandText, context);
    this.isSet = isSet;
    this.key = key;
    this.value = value;
  }

  public void run() throws OdpsException, ODPSConsoleException {

    if (isSet) {

      if (key.equalsIgnoreCase("odps.instance.priority")) {
        try {
          getContext().setPriority(Integer.parseInt(value));
          getContext().setPaiPriority(Integer.parseInt(value));
        } catch (NumberFormatException e) {
          throw new ODPSConsoleException("priority need int value[odps.instance.priority=" + value
              + "]");
        }
      }

      if (key.equalsIgnoreCase("odps.running.cluster")) {
        getContext().setRunningCluster(value);
      }

      if (key.equalsIgnoreCase(SQL_TIMEZONE_FLAG)) {
        getContext().setSqlTimezone(value);
      }

      // set query results fetched by instance tunnel or not
      if (key.equalsIgnoreCase("console.sql.result.instancetunnel")) {
        getContext().setUseInstanceTunnel(Boolean.parseBoolean(value));
      }

      if (aclList.contains(key.toUpperCase())) {
        setSecurityConfig(key);
      } else {
        setMap.put(key, value);
      }
    } else {

      // 不能够传递
      aliasMap.put(key, value);
    }

    getWriter().writeError("OK");
  }

  // set SecurityConfig
  private void setSecurityConfig(String key) throws ODPSConsoleException, OdpsException {
    String project = getCurrentProject();
    Odps odps = getCurrentOdps();
    SecurityManager securityManager = odps.projects().get(project).getSecurityManager();
    SecurityConfiguration securityConfig = securityManager.getSecurityConfiguration();
    if (!(value.toLowerCase().indexOf("with") > 0))
      isBooleanStr(value);
    if (key.toUpperCase().equals("PROJECTPROTECTION")) {
      if (value.toLowerCase().indexOf("with") > 0) {
        isBooleanStr(value.substring(0, value.toLowerCase().indexOf("with")).trim());
        String fileName = value.substring(value.toLowerCase().indexOf("exception") + 9).trim();
        securityConfig.enableProjectProtection(FileUtil.getStringFromFile(fileName));
      } else {
        Boolean enable = Boolean.parseBoolean(value);
        if (enable) {
          securityConfig.enableProjectProtection();
        } else {
          securityConfig.disableProjectProtection();
        }
      }
    }

    Boolean enable = Boolean.parseBoolean(value);

    if (key.toUpperCase().equals("OBJECTCREATORHASACCESSPERMISSION")) {
      if (enable) {
        securityConfig.enableObjectCreatorHasAccessPermission();
      } else {
        securityConfig.disableObjectCreatorHasAccessPermission();
      }
    } else if (key.toUpperCase().equals("OBJECTCREATORHASGRANTPERMISSION")) {
      if (enable) {
        securityConfig.enableObjectCreatorHasGrantPermission();
      } else {
        securityConfig.disableObjectCreatorHasGrantPermission();
      }
    } else if (key.toUpperCase().equals("CHECKPERMISSIONUSINGACL")) {
      if (enable) {
        securityConfig.enableCheckPermissionUsingAcl();
      } else {
        securityConfig.disableCheckPermissionUsingAcl();
      }
    } else if (key.toUpperCase().equals("CHECKPERMISSIONUSINGPOLICY")) {
      if (enable) {
        securityConfig.enableCheckPermissionUsingPolicy();
      } else {
        securityConfig.disableCheckPermissionUsingPolicy();
      }
    } else if (key.toUpperCase().equals("LABELSECURITY")) {
      if (enable) {
        securityConfig.enableLabelSecurity();
      } else {
        securityConfig.disableLabelSecurity();
      }
    }

    securityManager.setSecurityConfiguration(securityConfig);
  }

  /*
   * 判断安全配置的set命令set xxx=true|false,是否是正确的ture或false字符串
   */
  private void isBooleanStr(String str) throws ODPSConsoleException {
    if (!str.toUpperCase().equals("TRUE") && !str.toUpperCase().equals("FALSE"))
      throw new ODPSConsoleException("SecurityConfig must be boolean String(set XXX=true|false)"
          + ODPSConsoleConstants.BAD_COMMAND);
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static SetCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    SetCommand command = null;

    if (commandString.toUpperCase().matches("^SET\\s+\\S+\\s*=\\s*\\S+.*")) {

      String keyValue = commandString.substring(3).trim();
      String[] temp = keyValue.split("=", 2);

      if (temp.length == 2) {
        command = new SetCommand(true, temp[0].trim(), temp[1].trim(), commandString,
            sessionContext);
      } else {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }

    } else if (commandString.toUpperCase().matches("^ALIAS\\s+\\S+\\s*=\\s*\\S+.*")) {

      String keyValue = commandString.substring(5).trim();
      String[] temp = keyValue.split("=");

      if (temp.length == 2) {
        command = new SetCommand(false, temp[0].trim(), temp[1].trim(), commandString,
            sessionContext);
      } else {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
    }
    return command;
  }

}
