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

package com.aliyun.openservices.odps.console.pub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsHook;
import com.aliyun.odps.OdpsHooks;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/4/15.
 */
public class WaitCommandTest {
  private static String TEST_TABLE_NAME = "test_wait_command";

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);

    if (!odps.tables().exists(TEST_TABLE_NAME)) {
      TableSchema schema = new TableSchema();
      schema.addColumn(new Column("col1", TypeInfoFactory.BIGINT));
      odps.tables().create(TEST_TABLE_NAME, schema);
    }
  }

  @AfterClass
  public static void tearDown() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);

    if (odps.tables().exists(TEST_TABLE_NAME)) {
      odps.tables().delete(TEST_TABLE_NAME);
    }
  }

  @Test
  public void testWaitCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    Instance instance = SQLTask.run(odps, "select count(*) from " + TEST_TABLE_NAME + ";");
    String cmd = "Wait " + instance.getId();
    WaitCommand command = WaitCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testWaitCommandWithHook() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    context.setOdpsHooks(null);
    Instance instance = SQLTask.run(odps, "select count(*) from " + TEST_TABLE_NAME + ";");
    instance.waitForSuccess();

    String cmd = "Wait " + instance.getId() + " -hooks";
    WaitCommand command = WaitCommand.parse(cmd, context);
    assertNotNull(command);

    context.setOdpsHooks("com.aliyun.openservices.odps.console.pub.TestOdpsHook");

    command.run();
    assertEquals(0, TestOdpsHook.beforeCallCounter);
    assertEquals(1, TestOdpsHook.afterCallCounter);

    context.setOdpsHooks(null);
  }
}
