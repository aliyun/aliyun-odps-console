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
import com.aliyun.odps.security.SecurityConfiguration;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;

public class ShowSecurityConfigurationCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "securityconfiguration", "auth"};

  public static void printUsage(PrintStream out) {
    out.println("Usage: show securityconfiguration");
  }

  public ShowSecurityConfigurationCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    SecurityManager sm = odps.projects().get().getSecurityManager();
    SecurityConfiguration securityConfig = sm.getSecurityConfiguration();

    DefaultOutputWriter outputWriter = this.getContext().getOutputWriter();
    outputWriter.writeResult("CheckPermissionUsingAcl=" + securityConfig.checkPermissionUsingAcl());
    outputWriter.writeResult("CheckPermissionUsingPolicy=" + securityConfig.checkPermissionUsingPolicy());
    outputWriter.writeResult("ObjectCreatorHasAccessPermission=" + securityConfig.objectCreatorHasAccessPermission());
    outputWriter.writeResult("ObjectCreatorHasGrantPermission=" + securityConfig.objectCreatorHasGrantPermission());
    outputWriter.writeResult("LabelSecurity=" + securityConfig.labelSecurity());

    outputWriter.writeResult("ProjectProtection=" + securityConfig.projectProtection());
    try {
      if (securityConfig.getProjectProtectionExceptionPolicy() != null
          && !securityConfig.getProjectProtectionExceptionPolicy().equals(""))
        outputWriter.writeResult("ProjectProtection with Exception:"
            + securityConfig.getProjectProtectionExceptionPolicy());
    } catch (OdpsException e) {
      // DO NOTHING FOR NO POLICY EXIST
    }
    if (!StringUtils.isNullOrEmpty(securityConfig.getAuthorizationVersion())) {
      outputWriter.writeResult("AuthorizationVersion=" + securityConfig.getAuthorizationVersion());
    }
  }

  public static ShowSecurityConfigurationCommand parse(String commandString, ExecutionContext context) {

    if (commandString.toUpperCase().matches("\\s*SHOW\\s+SECURITYCONFIGURATION\\s*")) {
      return new ShowSecurityConfigurationCommand(commandString, context);
    }
    return null;
  }
}
