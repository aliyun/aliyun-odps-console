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

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class UnSetCommandTest {

  @Test
  public void positiveTest() throws Exception {

    ExecutionContext context = ExecutionContext.init();
    SetCommand sc = SetCommand.parse("set testkey=testvalue", context);
    sc.run();
    assertEquals(1, SetCommand.setMap.size());

    sc = SetCommand.parse("alias testkey2=testvalue2", context);
    sc.run();
    assertEquals(1, SetCommand.aliasMap.size());

    UnSetCommand cmd = UnSetCommand.parse("unset testkey1 ", context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals(1, SetCommand.setMap.size());

    cmd = UnSetCommand.parse("unset testkey ", context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals(0, SetCommand.setMap.size());

    cmd = UnSetCommand.parse("UnseT testkey\n\r", null);
    assertNotNull(cmd);

    cmd = UnSetCommand.parse("UNSET testkey\t", null);
    assertNotNull(cmd);

    cmd = UnSetCommand.parse("UNSET test.key\t", null);
    assertNotNull(cmd);

    // alias
    cmd = UnSetCommand.parse("unalias testkey", context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals(1, SetCommand.aliasMap.size());

    cmd = UnSetCommand.parse("Unalias testkey2 \n\r", context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals(0, SetCommand.aliasMap.size());

    cmd = UnSetCommand.parse("Unalias test.key \n\r", null);
    assertNotNull(cmd);
  }

  @Test
  public void negativeTest() throws ODPSConsoleException, OdpsException {
    UnSetCommand cmd = UnSetCommand.parse("unse testkey", null);
    assertNull(cmd);

    cmd = UnSetCommand.parse("unset ", null);
    assertNull(cmd);

    cmd = UnSetCommand.parse("unalia testkey\n", null);
    assertNull(cmd);

    cmd  = UnSetCommand.parse("unalias \r\n", null);
    assertNull(cmd);
  }

  @Test
  public void testRunningCluster() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    UnSetCommand command = UnSetCommand.parse("unset odps.running.cluster", context);
    command.run();
    assertNull(context.getRunningCluster());
  }

  @Test
  public void testInstancePriority() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    UnSetCommand command = UnSetCommand.parse("unset odps.instance.priority", context);
    command.run();
    assertEquals(context.DEFAULT_PRIORITY, context.getPriority());
  }
}
