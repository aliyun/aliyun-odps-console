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

package com.aliyun.openservices.odps.console;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.aliyun.odps.OdpsException;

/**
 * Created by nizheming on 15/6/1.
 */
public class SQLCostCommandTest {

  @Test
  public void testSQLCostCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "COST SQL select count(key) from src;";
    SQLCostCommand command = SQLCostCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testSQLCostLongSpace() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "COST  \r\t\n    SQL   \r\n\t select count(key) from src;";
    SQLCostCommand command = SQLCostCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }


  @Test(expected = ODPSConsoleException.class)
  public void testParseSQLCostCommnad() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "COST SQL";
    SQLCostCommand command = SQLCostCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testSQLSelectAll() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "COST SQL select * from src;";
    SQLCostCommand command = SQLCostCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }

  @Test
  public void testSQLjoin() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    String cmd = "COST SQL select * from src a join src b on a.key=b.key;";
    SQLCostCommand command = SQLCostCommand.parse(cmd, context);
    assertNotNull(command);
    command.run();
  }
}