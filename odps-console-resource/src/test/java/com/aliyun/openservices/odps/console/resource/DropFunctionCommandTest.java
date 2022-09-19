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

package com.aliyun.openservices.odps.console.resource;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.Rule;

public class DropFunctionCommandTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testDropFunctionCommandPositive() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    String[] positive = new String[]{
        "drop function test_drop",
        "drop function a.b",
        "drop function a.b.c",
        "drop function if exists test_drop",
        "drop \t\nfunction test_linebreak"
    };
    for (String cmd : positive) {
      DropFunctionCommand command = DropFunctionCommand.parse(cmd, ctx);
      Assert.assertNotNull(command);
    }
  }

  @Test
  public void testDropFunctionCommandNegtive() throws ODPSConsoleException {
    thrown.expect(ODPSConsoleException.class);
    ExecutionContext ctx = ExecutionContext.init();
    String[] negative = new String[]{
        "drop function test_drop",
        "drop function if exists test_drop",
        "drop function a b",
        "drop function a.b.c.d",
        "drop function ",
        "drop resource a"
    };
    for (String cmd : negative) {
    }
    DropFunctionCommand command = null;
    command = DropFunctionCommand.parse("drop function teststee test_drop", ctx);
  }

}
