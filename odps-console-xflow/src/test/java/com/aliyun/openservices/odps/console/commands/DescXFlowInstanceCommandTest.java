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

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.xflow.DescXFlowInstanceCommand;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;

public class DescXFlowInstanceCommandTest {
  @Rule
  public ExpectedException expectEx = ExpectedException.none();

  @Test
  public void testDescXFlowInstanceCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc instance 123456";
    DescXFlowInstanceCommand command = DescXFlowInstanceCommand.parse(cmd, context);
    assertNull(command);
  }

  @Test
  public void testDescXFlowInstanceCommandProjectOnly() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc instance -p algo_test";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + " Invalid Instance Id.");
    DescXFlowInstanceCommand command = DescXFlowInstanceCommand.parse(cmd, context);
    assertNull(command);
  }

  @Test
  public void testNullCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "desc instance";
    expectEx.expect(ODPSConsoleException.class);
    expectEx.expectMessage(ODPSConsoleConstants.BAD_COMMAND + " need specify a Instance Id.");
    DescXFlowInstanceCommand command = DescXFlowInstanceCommand.parse(cmd, context);
    assertNull(command);
  }
}
