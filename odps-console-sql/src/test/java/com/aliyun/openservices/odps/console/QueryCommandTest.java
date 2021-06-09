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

import java.util.HashMap;
import java.util.Map;

import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;

public class QueryCommandTest {

  public class MockQueryCommand extends QueryCommand {

    public MockQueryCommand(boolean isSelect, String commandText, ExecutionContext context) {
      super(isSelect, commandText, context);
    }

    @Override
    protected void runJob(Task task) throws OdpsException, ODPSConsoleException {
      Map<String, String> setting = new HashMap<String, String>();
      setting.put("odps.sql.select.output.format", "HumanReadable");

      String res = new GsonBuilder().disableHtmlEscaping().create().toJson(setting);
      Assert
          .assertEquals(res, task.getProperties().get("settings"));
    }
  }

  private String[]
      selectCommandStrings = {"select count(*) from src;", "select\n\r count(*) \t from src;\t"};

  private String[]
      queryCommandStrings =
      {"create table test(cmd String);", "create\t\r\n table \ntest(cmd String);"};

  @Test
  public void testParse() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : selectCommandStrings) {
      QueryCommand command = QueryCommand.parse(cmd, context);
      assertNotNull(command);
      Assert.assertTrue(command.isSelectCommand());
    }

    for (String cmd : queryCommandStrings) {
      QueryCommand command = QueryCommand.parse(cmd, context);
      assertNotNull(command);
      Assert.assertFalse(command.isSelectCommand());
    }
  }

  @Test
  public void testRun() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();

    MockQueryCommand command = new MockQueryCommand(true, selectCommandStrings[0], context);
    command.run();

    command = new MockQueryCommand(false, queryCommandStrings[0], context);
    command.run();
  }
}
