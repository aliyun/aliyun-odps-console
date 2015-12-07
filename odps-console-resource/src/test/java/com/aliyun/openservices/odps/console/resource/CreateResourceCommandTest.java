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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class CreateResourceCommandTest {

  @Test
  public void testCreateResourceCommandParser() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    // test 1
    commandText = "create resource -f jar my_jar.jar";
    command = CreateResourceCommand.parse(commandText, context);

    assertEquals(command.getRefName(), "my_jar.jar");
    assertEquals(command.getAlias(), "");
    assertEquals(command.getComment(), null);
    assertEquals(command.getType(), "jar");
    assertEquals(command.getPartitionSpec(), "");
    assertEquals(command.isUpdate, true);

    // test 2
    commandText = "create resource table my_project2.my_table_2";
    command = CreateResourceCommand.parse(commandText, context);

    assertEquals(command.getRefName(), "my_project2.my_table_2");
    assertEquals(command.getAlias(), "");
    assertEquals(command.getComment(), null);
    assertEquals(command.getType(), "table");
    assertEquals(command.getPartitionSpec(), "");
    assertEquals(command.isUpdate, false);

    // test 3
    commandText = "create resource table my_part_table(pt='1') my_part_res";
    command = CreateResourceCommand.parse(commandText, context);

    assertEquals(command.getRefName(), "my_part_table");
    assertEquals(command.getAlias(), "my_part_res");
    assertEquals(command.getComment(), null);
    assertEquals(command.getType(), "table");
    assertEquals(command.getPartitionSpec(), "pt='1'");
    assertEquals(command.isUpdate, false);

    // test 4
    commandText = "create resource file -p my_project my_file.txt my_file_name";
    command = CreateResourceCommand.parse(commandText, context);

    assertEquals(command.getRefName(), "my_file.txt");
    assertEquals(command.getAlias(), "my_file_name");
    assertEquals(command.getComment(), null);
    assertEquals(command.getType(), "file");
    assertEquals(command.getPartitionSpec(), "");
    assertEquals(command.isUpdate, false);

    // test 5
    commandText = "create resource table my_part_table() my_part_res";
    command = CreateResourceCommand.parse(commandText, context);

    assertEquals(command.getRefName(), "my_part_table");
    assertEquals(command.getAlias(), "my_part_res");
    assertEquals(command.getComment(), null);
    assertEquals(command.getType(), "table");
    assertEquals(command.getPartitionSpec(), "");
    assertEquals(command.isUpdate, false);
  }

  @Test(expected = ODPSConsoleException.class)
  public void testCreateResourceCommandParserWithException() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "create resource file -wrong";
    command = CreateResourceCommand.parse(commandText, context);
  }

  @Test(expected = ODPSConsoleException.class)
  public void testCreateResourceCommandParserWithException2() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "create resource file aaa bbb ccc ddd eee";
    command = CreateResourceCommand.parse(commandText, context);
  }

  @Test(expected = ODPSConsoleException.class)
  public void testCreateResourceCommandParserWithParenUnmatched() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "create resource table my_part_table(pt='1' my_part_res";

    command = CreateResourceCommand.parse(commandText, context);

  }

  @Test(expected = ODPSConsoleException.class)
  public void testCreateResourceCommandParserWithParenWithoutAlias() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    String commandText = null;
    AddResourceCommand command;

    commandText = "create resource table my_part_table(pt='1')";

    command = CreateResourceCommand.parse(commandText, context);

  }
}
