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

import java.io.PrintStream;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.FileUtil;

public class PutPolicyCommand extends AbstractCommand {
  private String policyPath;
  private String roleName = null;

  public static final String[] HELP_TAGS = new String[]{"put", "policy"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: put policy <policyFile>");
    stream.println("       put policy <policyFile> on role <roleName>");
  }

  public String getPolicyPath() {
    return policyPath;
  }

  public String getRoleName() {
    return roleName;
  }

  public PutPolicyCommand(String policyPath, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.policyPath = policyPath;
  }

  public PutPolicyCommand(String policyPath, String roleName, String commandText,
      ExecutionContext context) {
    super(commandText, context);
    this.policyPath = policyPath;
    this.roleName = roleName;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();
    SecurityManager sm = odps.projects().get().getSecurityManager();

    String policy = FileUtil.getStringFromFile(policyPath);
    if (roleName == null) {
      sm.putProjectPolicy(policy);
    } else {
      sm.putRolePolicy(roleName, policy);
    }

    getWriter().writeError("OK");

  }

  public static PutPolicyCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    commandString = commandString.trim();
    if (commandString.toUpperCase().matches("PUT\\s+POLICY\\s+.*")) {

      String[] splits = commandString.split("\\s+");

      if (splits.length == 3) {
        return new PutPolicyCommand(splits[2], commandString, sessionContext);
      }
      if (splits.length == 6 && splits[3].toUpperCase().equals("ON")
          && splits[4].toUpperCase().equals("ROLE")) {
        // put policy on role
        return new PutPolicyCommand(splits[2], splits[5], commandString, sessionContext);
      } else
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }
    return null;
  }
}
