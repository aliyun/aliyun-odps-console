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

package com.aliyun.openservices.odps.console.xflow;

import static org.junit.Assert.*;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class DropOnlineModelCommandTest {
  
  @Test
  public void testDropOnlineModel() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "Drop OnlineModel IF EXISTS xxx";
    DropOnlineModelCommand command = DropOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }
  
  @Test(expected = OdpsException.class)
  public void testDropNotExists() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "Drop OnlineModel not_exists";
    DropOnlineModelCommand command = DropOnlineModelCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }
  
  @Test
  public void testNegativeCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "DropOnlineModel not_exists";
    DropOnlineModelCommand command = DropOnlineModelCommand.parse(cmd, context);
    assertNull(command);
  }

}
