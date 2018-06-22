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

}
