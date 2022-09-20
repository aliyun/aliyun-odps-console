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

package com.aliyun.openservices.odps.console.auth;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

import org.jline.reader.UserInterruptException;

public class AuthorizationCommand extends AbstractCommand {

  private static final Pattern[] PATTERNS = {
      Pattern.compile("\\s*GRANT.*"),
      Pattern.compile("\\s*REVOKE.*"),
      Pattern.compile("\\s*SHOW\\s+(GRANTS|ACL|PACKAGE|LABEL|ROLE|PRINCIPALS|PRIV).*"),
      Pattern.compile("\\s*CLEAR\\s+EXPIRED\\s+GRANTS\\s*"),
      Pattern.compile("\\s*LIST\\s+(USERS|ROLES|TRUSTEDPROJECTS|ACCOUNTPROVIDERS|GROUPS|TENANT USERS)\\s*"),
      Pattern.compile("\\s*CREATE\\s+(ROLE|PACKAGE|TENANT ROLE)\\s+.*"),
      Pattern.compile("\\s*DROP\\s+(ROLE|PACKAGE|TENANT ROLE)\\s+.*"),
      Pattern.compile("\\s*ADD\\s+(USER|TRUSTEDPROJECT|ACCOUNTPROVIDER|GROUP|TENANT USER|TENANT ROLE)\\s+.*"),
      Pattern.compile("\\s*ADD.*TO\\s+PACKAGE.*"),
      Pattern.compile("\\s*REMOVE\\s+(USER|TRUSTEDPROJECT|ACCOUNTPROVIDER|GROUP|TENANT USER|TENANT ROLE)\\s+.*"),
      Pattern.compile("\\s*REMOVE.*FROM\\s+PACKAGE.*"),
      Pattern.compile("\\s*ALTER\\s+(USER|GROUP)\\s+.*"),
      Pattern.compile("\\s*(DESCRIBE|DESC)\\s+(ROLE|PACKAGE|TENANT ROLE)\\s+.*"),
      Pattern.compile("\\s*DELETE\\s+PACKAGE\\s+.*"),
      Pattern.compile("\\s*(ALLOW|DISALLOW)\\s+PROJECT.*"),
      Pattern.compile("\\s*(INSTALL|UNINSTALL)\\s+PACKAGE.*"),
      Pattern.compile("\\s*SET\\s+(LABEL).*"),
      Pattern.compile("\\s*UNSET\\s+(LABEL).*")
  };

  public AuthorizationCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();

    SecurityManager sm = odps.projects().get().getSecurityManager();
    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();

    Map<String, String> settings = new HashMap<>();
    settings.put(ODPSConsoleConstants.ODPS_NAMESPACE_SCHEMA,
                 String.valueOf(getContext().isOdpsNamespaceSchema()));
    if (SetCommand.setMap.containsKey("odps.sql.allow.namespace.schema")) {
      settings.put("odps.sql.allow.namespace.schema", String.valueOf(getContext().isOdpsNamespaceSchema()));
    }
    settings.put(SetCommand.SQL_DEFAULT_SCHEMA, getContext().getSchemaName());
    SecurityManager.AuthorizationQueryInstance instance = sm.run(getCommandText(), false, null, settings);
    // SecurityManager.AuthorizationQueryInstance instance = sm.run(getCommandText(), false);


    while (!instance.isTerminated()) {
      outputWriter.writeError("waiting...");
      try {
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
      } catch (InterruptedException e) {
        throw new UserInterruptException("User interrupted.");
      }
    }

    String result = instance.getResult();
    if (instance.getStatus() == SecurityManager.AuthorizationQueryStatus.FAILED) {
      throw new ODPSConsoleException("Failed: " + result);
    }

    // 如果为空,则打印出OK
    if (StringUtils.isNullOrEmpty(result)) {
      outputWriter.writeResult("OK");
    } else {
      outputWriter.writeResult(result);
    }
  }

  public static AuthorizationCommand parse(String commandString, ExecutionContext sessionContext) {
    assert (commandString != null);
    String command = commandString.toUpperCase();

    for (Pattern pattern : PATTERNS) {
      if (pattern.matcher(command).matches()) {
        return new AuthorizationCommand(commandString, sessionContext);
      }
    }
    return null;
  }
}
