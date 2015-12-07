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
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class GetPolicyCommand extends AbstractCommand {

  private String roleName = null;

  public String getRoleName() {
    return roleName;
  }

  public GetPolicyCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public GetPolicyCommand(String roleName, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.roleName = roleName;

  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    SecurityManager sm = odps.projects().get().getSecurityManager();

    String policy = "";
    if (roleName == null) {
      policy = sm.getProjectPolicy();
    } else {
      policy =sm.getRolePolicy(roleName);
    }

    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    outputWriter.writeResult(policy);

  }

  public static GetPolicyCommand parse(String commandString, ExecutionContext sessionContext) {
    if (commandString.trim().toUpperCase().matches("GET\\s+POLICY.*")) {

      String[] splits = commandString.split("\\s+");
      if (splits.length == 2) {
        return new GetPolicyCommand(commandString, sessionContext);
      } else if (splits.length == 5 && splits[2].toUpperCase().equals("ON")
          && splits[3].toUpperCase().equals("ROLE")) {

        return new GetPolicyCommand(splits[4], commandString, sessionContext);
      }

    }
    return null;
  }

}
