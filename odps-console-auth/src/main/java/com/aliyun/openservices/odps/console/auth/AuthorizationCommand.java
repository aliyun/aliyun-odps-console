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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class AuthorizationCommand extends AbstractCommand {

  public AuthorizationCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();

    SecurityManager sm = odps.projects().get().getSecurityManager();

    String result = sm.runQuery(getCommandText(), false);

    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();

    // 如果为空,则打印出OK
    if (StringUtils.isNullOrEmpty(result)) {
      outputWriter.writeResult("OK");
    } else {
      outputWriter.writeResult(result);
    }

  }

  public static AuthorizationCommand parse(String commandString, ExecutionContext sessionContext) {
    assert (commandString != null);
    String tempString = commandString.toUpperCase();

    String patternArray[] = { "\\s*GRANT.*", "\\s*REVOKE.*",
        "\\s*SHOW\\s+(GRANTS|ACL|PACKAGES|LABEL).*", "\\s*SHOW\\s+PRIVILEGES\\s*",
        "\\s*SHOW\\s+PRIV\\s*", "\\s*CLEAR\\s+EXPIRED\\s+GRANTS\\s*",
        "\\s*LIST\\s+(USERS|ROLES|TRUSTEDPROJECTS)\\s*", "\\s*CREATE\\s+ROLE\\s+.*",
        "\\s*DROP\\s+ROLE\\s+.*", "\\s*ADD\\s+(USER|TRUSTEDPROJECT)\\s+.*",
        "\\s*REMOVE\\s+(USER|TRUSTEDPROJECT)\\s+.*", "\\s*(DESCRIBE|DESC)\\s+(ROLE|PACKAGE)\\s+.*",
        "\\s*(CREATE|DELETE|DROP)\\s+PACKAGE\\s+.*", "\\s*ADD.*TO\\s+PACKAGE.*",
        "\\s*REMOVE.*FROM\\s+PACKAGE.*", "\\s*(ALLOW|DISALLOW)\\s+PROJECT.*",
        "\\s*(INSTALL|UNINSTALL)\\s+PACKAGE.*", "\\s*LIST\\s+ACCOUNTPROVIDERS\\s*",
        "\\s*ADD\\s+ACCOUNTPROVIDER\\s+.*", "\\s*REMOVE\\s+ACCOUNTPROVIDER\\s+.*",
        "\\s*SET\\s+(LABEL).*" };
    for (String pattern : patternArray) {
      if (tempString.matches(pattern))
        return new AuthorizationCommand(commandString, sessionContext);
    }
    return null;
  }

}
