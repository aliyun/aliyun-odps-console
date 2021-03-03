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

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class SetCommandTest {

  @Test
  public void test() throws Exception {

    SetCommand sc = SetCommand.parse("set testkey=testvalue", null);
    assertEquals("testkey", sc.getKey());
    assertEquals("testvalue", sc.getValue());

    sc = SetCommand.parse("set testkey = testvalue", null);
    assertEquals("testkey", sc.getKey());
    assertEquals("testvalue", sc.getValue());

    sc = SetCommand.parse("set testkey =testvalue  ", null);
    assertEquals("testkey", sc.getKey());
    assertEquals("testvalue", sc.getValue());

    sc = SetCommand.parse("set test.key =test.value  ", null);
    assertEquals("test.key", sc.getKey());
    assertEquals("test.value", sc.getValue());

    sc = SetCommand.parse("set label xxx", null);
    assertNull(sc);

    sc = SetCommand.parse("set label= ", null);
    assertNull(sc);

    // alias
    sc = SetCommand.parse("alias testkey=testvalue", null);
    assertEquals("testkey", sc.getKey());
    assertEquals("testvalue", sc.getValue());

    sc = SetCommand.parse("alias testkey = testvalue", null);
    assertEquals("testkey", sc.getKey());
    assertEquals("testvalue", sc.getValue());

    sc = SetCommand.parse("alias test.key =test.value  ", null);
    assertEquals("test.key", sc.getKey());
    assertEquals("test.value", sc.getValue());

    sc = SetCommand.parse("alias label xxx", null);
    assertNull(sc);

    sc = SetCommand.parse("alias label= ", null);
    assertNull(sc);

  }

  @Test
  public void testSetPaiPriority() throws  ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    SetCommand command = SetCommand.parse("set odps.instance.priority=3", context);
    command.run();
    assertEquals(command.getContext().getPaiPriority(), Integer.valueOf(3));
  }

  @Test
  public void testRunningCluster() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    SetCommand command = SetCommand.parse("set odps.running.cluster = test_cluster", context);
    command.run();
    assertEquals(context.getRunningCluster(), "test_cluster");
    assertEquals(command.getContext().getPaiPriority(), Integer.valueOf(9));
    Odps odps = OdpsConnectionFactory.createOdps(context);
    assertEquals(odps.instances().getDefaultRunningCluster(), "test_cluster");
  }

}
