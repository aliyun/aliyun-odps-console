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

import java.lang.reflect.Field;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class ShowTablesCommandTest {

  private static String[] positives = {
      "SHOW TABLES",
      "show tables",
      " show tables \r",
      "show tables in project01_",
      "\n\r\t SHow\tTables  in \r project_01_sdf\n\r\t"};

  private static String[] negatives = {"show", "show table", "show tables in ", "show partition"};

  private static String[] pubPositives = {
      "LS TABLES",
      "ls tables",
      " ls tables \r",
      "ls tables -project=project01_",
      "\n\r\t Ls\tTables  -project=project_01_sdf\n\r\t",
      "LIST TABLES",
      "list tables",
      " list tables \r",
      "list tables -project=project01_",
      "\n\r\t LIsT\tTables  -project=project_01_sdf\n\r\t"};

  private static String[] pubNegatives =
      {"ls", "ls table", "ls partition", "list", "list table", "list partition"};

  private static String[] pubMatchProjects = {
      "ls tables -p project_test",
      "ls tables -project project_test",
      "ls tables -project=project_test",
      "ls tables -p=project_test",
      "list tables -p project_test",
      "list tables -project project_test",
      "list tables -project=project_test",
      "list tables -p=project_test"};

  private static String[] pubLackPara =
      {"ls tables -p", "ls tables -project", "list tables -p", "list tables -project"};

  private static String[] pubInvalidPara = {"ls tables xxx", "ls tables -xxx"};

  private static String[] prefixPositives = {"save%", "%", "jdbc%", "temp*", "abc*"};

  private static String[] prefixNegatives = {"!!!%"};

  private static String[] prefixMatchPositives = {
      "show tables in project_01_sdf like '%s'",
      "show tables from project_01_sdf like '%s'",
      "show tables like '%s'",
      "ShOw TAbLEs LIkE '%s'"};

  private static String[] prefixMatchNegatives = {
      "show table in project_01_sdf like '%s'",
      "show tables in project_01_sdf like %s",
      "show tables in project_01_sdf '%s'",
      "show tables in project_01_sdf %s'",
      "show tables in like '%s'"};

  @Test
  public void testMatchPositive() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : positives) {
      System.out.println(cmd);

      Assert.assertNotNull(ShowTablesCommand.parse(cmd, ctx));
      // all positive commands should match internal regex pattern
      // Assert.assertTrue(ShowTablesCommand.matchInternalCmd(cmd).matches());

      // all positive commands HERE should have NULL prefix name
      // Assert.assertNull(ShowTablesCommand.getPrefixName(ShowTablesCommand.matchInternalCmd(cmd)));
    }

    ShowTablesCommand command = ShowTablesCommand.parse("show tables", ctx);
    Assert.assertNotNull(command);
    command.run();
  }


  @Test
  public void testMatchNegative() throws ODPSConsoleException {
    for (String cmd : negatives) {
      Assert.assertNull(ShowTablesCommand.parse(cmd, ExecutionContext.init()));
    }
  }

  @Test
  public void testMatchPublicPositive() throws ODPSConsoleException {
    for (String cmd : pubPositives) {
      Assert.assertNotNull(ShowTablesCommand.parse(cmd, ExecutionContext.init()));
    }
  }

  @Test
  public void testMatchPublicNegative() throws ODPSConsoleException {
    for (String cmd : pubNegatives) {
      Assert.assertNull(ShowTablesCommand.parse(cmd, ExecutionContext.init()));
    }
  }

  @Test
  public void testLackProjectName() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : pubLackPara) {
      try {
        ShowTablesCommand command = ShowTablesCommand.parse(cmd, context);
        Assert.assertNotNull(null);
      } catch (ODPSConsoleException ignored) {
      }
    }
  }

  @Test
  public void testInvalidParams() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : pubInvalidPara) {
      try {
        ShowTablesCommand command = ShowTablesCommand.parse(cmd, context);
        Assert.assertNotNull(null);
      } catch (ODPSConsoleException e) {

      }
    }
  }

  @Test
  public void testPrefixPositives() throws ODPSConsoleException, NoSuchFieldException, IllegalAccessException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : prefixMatchPositives) {
      for (String prefix : prefixPositives) {
        String cmdStr = String.format(cmd, prefix);
        ShowTablesCommand command = ShowTablesCommand.parse(cmdStr, context);
        Assert.assertNotNull(command);
        Field field = command.getClass().getDeclaredField("prefix");
        field.setAccessible(true);
        Assert.assertEquals(prefix.substring(0, prefix.length()-1), field.get(command));
      }
    }
  }

  @Test
  public void testPrefixNegatives() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : prefixMatchNegatives) {
      for (String prefix : prefixNegatives) {
        String cmdStr = String.format(cmd, prefix);
        ShowTablesCommand command = ShowTablesCommand.parse(cmdStr, context);
        Assert.assertNull(command);
      }
    }
  }

}
