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

import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.CHECK_PERMISSION_USING_ACL;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.CHECK_PERMISSION_USING_POLICY;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.CONSOLE_SQL_RESULT_INSTANCETUNNEL;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.ENABLE_INTERACTIVE_MODE;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.EXTERNAL_RESOURCE_ACCESS_CONTROL;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.LABEL_SECURITY;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.OBJECT_CREATOR_HAS_ACCESS_PERMISSION;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.OBJECT_CREATOR_HAS_GRANT_PERMISSION;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.ODPS_DEFAULT_SCHEMA;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.ODPS_INSTANCE_PRIORITY;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.ODPS_NAMESPACE_SCHEMA;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.ODPS_RUNNING_CLUSTER;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.ODPS_SQL_TIMEZONE;
import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.PROJECT_PROTECTION;

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
 */
public class SetCommand extends AbstractCommand {

  protected static final String[] HELP_TAGS = new String[]{"set", "alias"};

  private static final String SET_REGEX = "^SET\\s+\\S+\\s*=\\s*\\S+.*";
  private static final String ALIAS_REGEX = "^ALIAS\\s+\\S+\\s*=\\s*\\S+.*";

  // session map
  public static Map<String, String> setMap = new HashMap<>();
  public static Map<String, String> aliasMap = new HashMap<>();

  private static final List<String>
      ACL_LIST =
      Arrays.asList(OBJECT_CREATOR_HAS_ACCESS_PERMISSION, OBJECT_CREATOR_HAS_GRANT_PERMISSION,
                    CHECK_PERMISSION_USING_ACL, CHECK_PERMISSION_USING_POLICY, PROJECT_PROTECTION,
                    LABEL_SECURITY, EXTERNAL_RESOURCE_ACCESS_CONTROL);

  private final boolean isSet;
  private final String key;
  private final String value;

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: set|alias <key>=<value>");
  }

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
      if (ODPS_INSTANCE_PRIORITY.equalsIgnoreCase(key)) {
        try {
          getContext().setPriority(Integer.parseInt(value));
          getContext().setPaiPriority(Integer.parseInt(value));
        } catch (NumberFormatException e) {
          throw new ODPSConsoleException(
              "priority need int value[odps.instance.priority=" + value + "]");
        }
      }

      if (ODPS_RUNNING_CLUSTER.equalsIgnoreCase(key)) {
        getContext().setRunningCluster(value);
      }

      if (ODPS_SQL_TIMEZONE.equalsIgnoreCase(key)) {
        getContext().setSqlTimezone(value);
        getContext().setUserSetSqlTimezone(true);
      }

      if (ENABLE_INTERACTIVE_MODE.equals(key)) {
        // change interactive mode temporarily
        getContext().setInteractiveQuery(Boolean.parseBoolean(value));
        getWriter().writeError("OK");
        return;
      }

      if (key.startsWith(ODPSConsoleConstants.FALLBACK_PREFIX)) {
        switch (key) {
          case ODPSConsoleConstants.FALLBACK_RESOURCE_NOT_ENOUGH:
            getContext().getFallbackPolicy()
                .fallback4ResourceNotEnough(Boolean.parseBoolean(value));
            break;
          case ODPSConsoleConstants.FALLBACK_UNSUPPORTED:
            getContext().getFallbackPolicy()
                .fallback4UnsupportedFeature(Boolean.parseBoolean(value));
            break;
          case ODPSConsoleConstants.FALLBACK_UPGRADING:
            getContext().getFallbackPolicy().fallback4Upgrading(Boolean.parseBoolean(value));
            break;
          case ODPSConsoleConstants.FALLBACK_QUERY_TIMEOUT:
            getContext().getFallbackPolicy().fallback4RunningTimeout(Boolean.parseBoolean(value));
            break;
          case ODPSConsoleConstants.FALLBACK_ATTACH_FAILED:
            getContext().getFallbackPolicy().fallback4AttachError(Boolean.parseBoolean(value));
            break;
          case ODPSConsoleConstants.FALLBACK_UNKNOWN:
            getContext().getFallbackPolicy().fallback4UnknownError(Boolean.parseBoolean(value));
            break;
          default:
            throw new ODPSConsoleException("unknown fallback policy: " + key);
        }
        getWriter().writeError("OK");
        return;
      }

      // set query results fetched by instance tunnel or not
      if (CONSOLE_SQL_RESULT_INSTANCETUNNEL.equalsIgnoreCase(key)) {
        getContext().setUseInstanceTunnel(Boolean.parseBoolean(value));
      }

      // This flag will also be set by odpscmd itself when users change the project and schema
      if (ODPS_DEFAULT_SCHEMA.equalsIgnoreCase(key)) {
        if (getContext().isProjectMode()) {
          throw new ODPSConsoleException(
              "Can't set default schema if odps.namespace.schema is false");
        }

        if (!getCurrentOdps().schemas().exists(value)) {
          throw new ODPSConsoleException(
              "schema " + value + " not exists in project " + getCurrentOdps().getDefaultProject());
        }

        getContext().setSchemaName(value);
      }
      if (ODPS_NAMESPACE_SCHEMA.equals(key)) {
        isBooleanStr(value, key);
        getContext().setOdpsNamespaceSchema(Boolean.parseBoolean(value));
      }

      if (ACL_LIST.contains(key.toUpperCase())) {
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
    if (!isStringContainingWith(value)) {
      isBooleanStr(value, key);
    }
    if (PROJECT_PROTECTION.equalsIgnoreCase(key)) {
      if (isStringContainingWith(value)) {
        isBooleanStr(value.substring(0, value.toLowerCase().indexOf("with")).trim(), key);
        String fileName = value.substring(value.toLowerCase().indexOf("exception") + 9).trim();
        securityConfig.enableProjectProtection(FileUtil.getStringFromFile(fileName));
      } else {
        boolean enable = Boolean.parseBoolean(value);
        if (enable) {
          securityConfig.enableProjectProtection();
        } else {
          securityConfig.disableProjectProtection();
        }
      }
    }

    if (EXTERNAL_RESOURCE_ACCESS_CONTROL.equalsIgnoreCase(key)) {
      if (isStringContainingWith(value)) {
        String prefix = value.substring(0, value.toLowerCase().indexOf("with")).trim();
        isBooleanStr(prefix, key);
        boolean
            enable =
            Boolean.parseBoolean(prefix);
        if (!enable) {
          throw new ODPSConsoleException(
              "External resource locations can only be set when external resource access control is enabled");
        }
        String locations = value.substring(value.toLowerCase().indexOf("locations") + 9).trim();
        securityConfig.enableExternalResourceAccessControl(locations);
      } else {
        boolean enable = Boolean.parseBoolean(value);
        if (enable) {
          securityConfig.enableExternalResourceAccessControl();
        } else {
          securityConfig.disableExternalResourceAccessControl();
        }
      }
    }

    boolean enable = Boolean.parseBoolean(value);

    if (OBJECT_CREATOR_HAS_ACCESS_PERMISSION.equalsIgnoreCase(key)) {
      if (enable) {
        securityConfig.enableObjectCreatorHasAccessPermission();
      } else {
        securityConfig.disableObjectCreatorHasAccessPermission();
      }
    } else if (OBJECT_CREATOR_HAS_GRANT_PERMISSION.equalsIgnoreCase(key)) {
      if (enable) {
        securityConfig.enableObjectCreatorHasGrantPermission();
      } else {
        securityConfig.disableObjectCreatorHasGrantPermission();
      }
    } else if (CHECK_PERMISSION_USING_ACL.equalsIgnoreCase(key)) {
      if (enable) {
        securityConfig.enableCheckPermissionUsingAcl();
      } else {
        securityConfig.disableCheckPermissionUsingAcl();
      }
    } else if (CHECK_PERMISSION_USING_POLICY.equalsIgnoreCase(key)) {
      if (enable) {
        securityConfig.enableCheckPermissionUsingPolicy();
      } else {
        securityConfig.disableCheckPermissionUsingPolicy();
      }
    } else if (LABEL_SECURITY.equalsIgnoreCase(key)) {
      if (enable) {
        securityConfig.enableLabelSecurity();
      } else {
        securityConfig.disableLabelSecurity();
      }
    }

    securityManager.setSecurityConfiguration(securityConfig);
  }

  /**
   * 判断安全配置的set命令set xxx=true|false,是否是正确的ture或false字符串
   */
  private static void isBooleanStr(String str, String key) throws ODPSConsoleException {
    if (!"TRUE".equalsIgnoreCase(str) && !"FALSE".equalsIgnoreCase(str)) {
      throw new ODPSConsoleException(
          key + " must be boolean String(set XXX=true|false) " + ODPSConsoleConstants.BAD_COMMAND);
    }
  }

  private static boolean isStringContainingWith(String value) {
    return value.toLowerCase().indexOf("with") >= 1;
  }

  /**
   * 通过传递的参数，解析出对应的command
   **/
  public static SetCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    SetCommand command = null;

    if (commandString.toUpperCase().matches(SET_REGEX)) {

      String keyValue = commandString.substring(3).trim();
      String[] temp = keyValue.split("=", 2);

      if (temp.length == 2) {
        command =
            new SetCommand(true, temp[0].trim(), temp[1].trim(), commandString, sessionContext);
      } else {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }

    } else if (commandString.toUpperCase().matches(ALIAS_REGEX)) {

      String keyValue = commandString.substring(5).trim();
      String[] temp = keyValue.split("=");

      if (temp.length == 2) {
        command =
            new SetCommand(false, temp[0].trim(), temp[1].trim(), commandString, sessionContext);
      } else {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
    }
    return command;
  }

}
