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

import org.apache.commons.lang.StringUtils;

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
 */
public class SetCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"set", "alias"};
  public static final String SQL_TIMEZONE_FLAG = "odps.sql.timezone";
  public static final String SQL_DEFAULT_SCHEMA = "odps.default.schema";

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: set|alias <key>=<value>");
  }

  // session map
  public static Map<String, String> setMap = new HashMap<>();
  public static Map<String, String> aliasMap = new HashMap<>();

  private static List<String> aclList = Arrays.asList(
      "OBJECTCREATORHASACCESSPERMISSION",
      "OBJECTCREATORHASGRANTPERMISSION",
      "CHECKPERMISSIONUSINGACL",
      "CHECKPERMISSIONUSINGPOLICY",
      "PROJECTPROTECTION",
      "LABELSECURITY",
      "EXTERNALRESOURCEACCESSCONTROL");

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

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    if (isSet) {
      if ("odps.instance.priority".equalsIgnoreCase(key)) {
        try {
          getContext().setPriority(Integer.parseInt(value));
          getContext().setPaiPriority(Integer.parseInt(value));
        } catch (NumberFormatException e) {
          throw new ODPSConsoleException("priority need int value[odps.instance.priority=" + value
                                         + "]");
        }
      }

      if ("odps.running.cluster".equalsIgnoreCase(key)) {
        getContext().setRunningCluster(value);
      }

      if (key.equalsIgnoreCase(SQL_TIMEZONE_FLAG)) {
        getContext().setSqlTimezone(value);
      }

      if (key.equals(ODPSConsoleConstants.ENABLE_INTERACTIVE_MODE)) {
        // change interactive mode temporarily
        getContext().setInteractiveQuery(Boolean.valueOf(value));
        getWriter().writeError("OK");
        return;
      }

      if (key.startsWith(ODPSConsoleConstants.FALLBACK_PREFIX)) {
        if (key.equals(ODPSConsoleConstants.FALLBACK_RESOURCE_NOT_ENOUGH)) {
          getContext().getFallbackPolicy().fallback4ResourceNotEnough(Boolean.valueOf(value));
        } else if (key.equals(ODPSConsoleConstants.FALLBACK_UNSUPPORTED)) {
          getContext().getFallbackPolicy().fallback4UnsupportedFeature(Boolean.valueOf(value));
        } else if (key.equals(ODPSConsoleConstants.FALLBACK_UPGRADING)) {
          getContext().getFallbackPolicy().fallback4Upgrading(Boolean.valueOf(value));
        } else if (key.equals(ODPSConsoleConstants.FALLBACK_QUERY_TIMEOUT)) {
          getContext().getFallbackPolicy().fallback4RunningTimeout(Boolean.valueOf(value));
        } else if (key.equals(ODPSConsoleConstants.FALLBACK_ATTACH_FAILED)) {
          getContext().getFallbackPolicy().fallback4AttachError(Boolean.valueOf(value));
        } else if (key.equals(ODPSConsoleConstants.FALLBACK_UNKNOWN)) {
          getContext().getFallbackPolicy().fallback4UnknownError(Boolean.valueOf(value));
        }
        getWriter().writeError("OK");
        return;
      }

      // set query results fetched by instance tunnel or not
      if ("console.sql.result.instancetunnel".equalsIgnoreCase(key)) {
        getContext().setUseInstanceTunnel(Boolean.parseBoolean(value));
      }

      if (aclList.contains(key.toUpperCase())) {
        setSecurityConfig(key);
      } else {
        setMap.put(key, value);
      }

      // This flag will also be set by odpscmd itself when users change the project and schema
      if (SQL_DEFAULT_SCHEMA.equalsIgnoreCase(key)) {
        if (getContext().isProjectMode()) {
          throw new ODPSConsoleException("Can't set default schema if odps.namespace.schema is false");
        }
        getContext().setSchemaName(value);
        setMap.put(key, value);
      }

      if (ODPSConsoleConstants.ODPS_NAMESPACE_SCHEMA.equals(key)) {
        isBooleanStr(value, key);
        getContext().setOdpsNamespaceSchema(Boolean.parseBoolean(value));
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
    if (!(value.toLowerCase().indexOf("with") > 0)) {
      isBooleanStr(value, key);
    }
    if ("PROJECTPROTECTION".equals(key.toUpperCase())) {
      if (value.toLowerCase().indexOf("with") > 0) {
        isBooleanStr(value.substring(0, value.toLowerCase().indexOf("with")).trim(), key);
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

    if ("EXTERNALRESOURCEACCESSCONTROL".equalsIgnoreCase(key)) {
      if (value.toLowerCase().indexOf("with") > 0) {
        isBooleanStr(value.substring(0, value.toLowerCase().indexOf("with")).trim(), key);
        Boolean enable =
            Boolean.parseBoolean(value.substring(0, value.toLowerCase().indexOf("with")).trim());
        if (!enable) {
          throw new ODPSConsoleException(
              "External resource locations can only be set when external resource access control is enabled");
        }
        String locations = value.substring(value.toLowerCase().indexOf("locations") + 9).trim();
        securityConfig.enableExternalResourceAccessControl(locations);
      } else {
        Boolean enable = Boolean.parseBoolean(value);
        if (enable) {
          securityConfig.enableExternalResourceAccessControl();
        } else {
          securityConfig.disableExternalResourceAccessControl();
        }
      }
    }

    Boolean enable = Boolean.parseBoolean(value);

    if ("OBJECTCREATORHASACCESSPERMISSION".equals(key.toUpperCase())) {
      if (enable) {
        securityConfig.enableObjectCreatorHasAccessPermission();
      } else {
        securityConfig.disableObjectCreatorHasAccessPermission();
      }
    } else if ("OBJECTCREATORHASGRANTPERMISSION".equals(key.toUpperCase())) {
      if (enable) {
        securityConfig.enableObjectCreatorHasGrantPermission();
      } else {
        securityConfig.disableObjectCreatorHasGrantPermission();
      }
    } else if ("CHECKPERMISSIONUSINGACL".equals(key.toUpperCase())) {
      if (enable) {
        securityConfig.enableCheckPermissionUsingAcl();
      } else {
        securityConfig.disableCheckPermissionUsingAcl();
      }
    } else if ("CHECKPERMISSIONUSINGPOLICY".equals(key.toUpperCase())) {
      if (enable) {
        securityConfig.enableCheckPermissionUsingPolicy();
      } else {
        securityConfig.disableCheckPermissionUsingPolicy();
      }
    } else if ("LABELSECURITY".equals(key.toUpperCase())) {
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
  private static void isBooleanStr(String str, String key) throws ODPSConsoleException {
    if (!"TRUE".equalsIgnoreCase(str) && !"FALSE".equalsIgnoreCase(str)) {
      throw new ODPSConsoleException(key + " must be boolean String(set XXX=true|false) "
                                     + ODPSConsoleConstants.BAD_COMMAND);
    }
  }

  /**
   * 通过传递的参数，解析出对应的command
   **/
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
