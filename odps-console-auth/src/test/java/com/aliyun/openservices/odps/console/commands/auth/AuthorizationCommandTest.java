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

package com.aliyun.openservices.odps.console.commands.auth;

import com.aliyun.openservices.odps.console.auth.GetSecurityPolicyCommand;
import com.aliyun.openservices.odps.console.auth.PutSecurityPolicyCommand;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.auth.AuthorizationCommand;

/**
 * Created by ruibo.lirb on 16-2-24.
 */
public class AuthorizationCommandTest {

  @Test
  public void testSecurityV2Command() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();

    AuthorizationCommand cmd1 = AuthorizationCommand.parse("show role grant user <username>", ctx);
    Assert.assertNotNull(cmd1);

    AuthorizationCommand cmd2 = AuthorizationCommand.parse("show principals <rolename>", ctx);
    Assert.assertNotNull(cmd2);

    AuthorizationCommand cmd3 = AuthorizationCommand.parse("show package <packagename>", ctx);
    Assert.assertNotNull(cmd3);
  }

  @Test
  public void testParseGetSecurityPolicyCommand() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();

    String getSecurityPolicyCommand = "get security policy";
    GetSecurityPolicyCommand cmd1 =
        GetSecurityPolicyCommand.parse(getSecurityPolicyCommand, ctx);
    Assert.assertNotNull(cmd1);

    GetSecurityPolicyCommand cmd2 =
        GetSecurityPolicyCommand.parse(getSecurityPolicyCommand.toUpperCase(), ctx);
    Assert.assertNotNull(cmd2);
  }

  @Test
  public void testParsePutSecurityPolicyCommand() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();

    String putSecurityPolicyCommand = "put security policy /path/to/policy.json";
    String badPutSecurityPolicyCommand = "put security policy";
    PutSecurityPolicyCommand cmd1 =
        PutSecurityPolicyCommand.parse(putSecurityPolicyCommand, ctx);
    Assert.assertNotNull(cmd1);

    PutSecurityPolicyCommand cmd2 =
        PutSecurityPolicyCommand.parse(putSecurityPolicyCommand.toUpperCase(), ctx);
    Assert.assertNotNull(cmd2);

    PutSecurityPolicyCommand cmd3 =
        PutSecurityPolicyCommand.parse(badPutSecurityPolicyCommand, ctx);
    Assert.assertNull(cmd3);
  }

  @Test
  public void testParseTenantCommand() throws ODPSConsoleException {
    String[] commands = new String[] {
        "add tenant user u",
        "remove tenant user u",
        "create tenant role r",
        "drop tenant role r",
        "describe tenant role r",
        "grant tenant role r to user u",
        "revoke tenant role r from user u",
        "list tenant users",
        "grant select on table p.t to tenant role r",
        "revoke select on table p.t from tenant role r",
        "show grants for tenant role r",
        "show grants for tenant user u",
        "show principals for tenant role r",
        "add tenant role r to project p",
        "remove tenant role r from project p"
    };

    ExecutionContext ctx = ExecutionContext.init();
    for (String command : commands) {
      Assert.assertNotNull(AuthorizationCommand.parse(command, ctx));
    }
  }
}
