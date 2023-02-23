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
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class ShowViewsCommandTest {

  private static String[] positives = {
          "SHOW VIEWS",
          "show views",
          " show views \r",
          "show materialized views",
          " show MATERIALIZED  VIEWS\r",
          " show MATERIALIZED  VIEWS\r in project_2.acb like 'bbb%'",
          "show views in project01_",
          "\n\r\t SHow\tViews  in \r project_01_sdf\n\r\t"};

  private static String[] negatives = {"show", "show view", "show mat view","show views in ", "show partition", "list views"};

  private static String[] prefixPositives = {"save%", "%", "jdbc%", "temp*", "abc*"};

  private static String[] prefixNegatives = {"!!!%"};

  private static String[] prefixMatchPositives = {
      "show %s views in project_01_sdf like '%s'",
      "show %s views like '%s'",
      "ShOw %s VieWs LIkE '%s'",
      "ShOw %s VieWs in project_01_sdf.schema_0 LIkE '%s'"};

  private static String[] prefixMatchNegatives = {
      "show %s views in project_01_sdf like '%s'",
      "show %s views in project_01_sdf like %s",
      "show %s views in project_01_sdf '%s'",
      "show %s views in project_01_sdf %s'",
      "show %s views in like '%s'"};

  @Test
  public void testMatchPositive() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : positives) {
      System.out.println(cmd);

      Assert.assertNotNull(ShowViewsCommand.parse(cmd, ctx));
      // all positive commands should match internal regex pattern
      // Assert.assertTrue(ShowViewsCommand.matchInternalCmd(cmd).matches());

      // all positive commands HERE should have NULL prefix name
      // Assert.assertNull(ShowViewsCommand.getPrefixName(ShowViewsCommand.matchInternalCmd(cmd)));
    }

    ShowViewsCommand command = ShowViewsCommand.parse("show views", ctx);
    Assert.assertNotNull(command);
    command.run();

    command = ShowViewsCommand.parse("show materialized views", ctx);
    Assert.assertNotNull(command);
    command.run();
  }


  @Test
  public void testMatchNegative() throws ODPSConsoleException {
    for (String cmd : negatives) {
      Assert.assertNull(ShowViewsCommand.parse(cmd, ExecutionContext.init()));
    }
  }

  @Test
  public void testPrefixPositives() throws ODPSConsoleException, NoSuchFieldException, IllegalAccessException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : prefixMatchPositives) {
      for (String prefix : prefixPositives) {
        String cmdStr = String.format(cmd, "", prefix);
        ShowViewsCommand command = ShowViewsCommand.parse(cmdStr, context);
        Assert.assertNotNull(command);

        Assert.assertEquals(prefix.substring(0, prefix.length()-1), command.getPrefix());
        Assert.assertEquals(Table.TableType.VIRTUAL_VIEW, command.getType());
      }
    }

  }

  @Test
  public void testPrefixNegatives() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : prefixMatchNegatives) {
      for (String prefix : prefixNegatives) {
        String cmdStr = String.format(cmd, "", prefix);
        ShowViewsCommand command = ShowViewsCommand.parse(cmdStr, context);
        Assert.assertNull(command);
      }
    }
  }

  @Test
  public void testMaterializedPrefixPositives() throws ODPSConsoleException, NoSuchFieldException, IllegalAccessException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : prefixMatchPositives) {
      for (String prefix : prefixPositives) {
        String cmdStr = String.format(cmd, "materialized ", prefix);
        ShowViewsCommand command = ShowViewsCommand.parse(cmdStr, context);
        Assert.assertNotNull(command);

        Assert.assertEquals(prefix.substring(0, prefix.length()-1), command.getPrefix());
        Assert.assertEquals(Table.TableType.MATERIALIZED_VIEW, command.getType());
      }
    }

  }

  @Test
  public void testMaterializedPrefixNegatives() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : prefixMatchNegatives) {
      for (String prefix : prefixNegatives) {
        String cmdStr = String.format(cmd, "materialized ", prefix);
        ShowViewsCommand command = ShowViewsCommand.parse(cmdStr, context);
        Assert.assertNull(command);
      }
    }
  }


}
