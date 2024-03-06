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
import static org.junit.Assert.assertNull;

import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.QueryCommand;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.common.CommandUtils;

public class DescribeTableCommandTest {

  private static final String[] positives = {
      "DESCRIBE project_name.table_name",
      "DESC project_name.table_name",
      "desc project_name.table_name",
      "desc project_name.schema_name.table_name",
      "\r\n\tDesCribe\r\n\tproject_name.table_name\r\t\n",
      "desc extended project_name.table_name",
      "\r\n\tDesCribe\r\n\textended\r\n\tproject_name.table_name\r\t\n",
      };

  private static final String[] p_positives = {
      "DESCRIBE project_name.table_name PARTITION(pt='1', dt='1')",
      "DESC project_name.table_name PARTITION(pt='1', dt='1')",
      "desc project_name.table_name partition(pt='1', dt='1')",
      "desc project_name.table_name partition(pt=\"1\", dt=\"1\")",
      "desc project_name.table_name partition(pt='1', dt=\"1\")",
      "\r\t\n  DesCribe\r\n\tproject_name.table_name\r\t\nPARTITION\t\n(pt='1', dt='1')\t\n"};

  private static final String[] not_match_negatives = {
      "DESCRIBE",
      "DESC",
      "DESC ",
      "DESCRIBE ",
      "DESC project_name.table_name PARTITIONS (pt='x', dt='x')",
      "desc function a",
      "desc schema a",
      };

  private static final String[] match_but_error_negatives = {
      "desc a.b.c.d",
      "desc a.b.",
      "desc a..b"
  };

  @Test
  public void testCommandParse() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : p_positives) {
      AbstractCommand command = DescribeTableCommand.parse(cmd, context);
      assertNotNull(command);
    }

    for (String cmd : positives) {
      AbstractCommand command = DescribeTableCommand.parse(cmd, context);
      assertNotNull(command);
    }
  }

  @Test
  public void testCommandNegative() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : not_match_negatives) {
      AbstractCommand command = DescribeTableCommand.parse(cmd, context);
      assertNull(command);
    }

    int err_count = 0;
    for (String command : match_but_error_negatives) {
      try {
        DescribeTableCommand.parse(command, context);
      } catch (ODPSConsoleException e) {
        err_count++;
      }
    }

    assertEquals(match_but_error_negatives.length, err_count);
  }

  @Test
  public void testStorageTier() throws OdpsException, ODPSConsoleException {
    // 测试分层存储的功能是否能走通. 不出现异常
    ExecutionContext context = ExecutionContext.init();
    Function<String, String> deleteTable = (table) -> ("drop table if exists " + table + ";");
    String plainTable = "plain_table_" + CommandUtils.getRandomName();
    String partitionTable = "partition_table_" + CommandUtils.getRandomName();
    boolean ok = true;
    try {
      SetCommand setCommand = SetCommand.parse("set odps.tiered.storage.enable=true", context);
      setCommand.run();

      QueryCommand.parse("create table if not exists " + plainTable
                         + " (id bigint,name string) TBLPROPERTIES ('storagetier'='lowfrequency');",
                         context).run();
      QueryCommand.parse("create table if not exists " + partitionTable
                         + " (id bigint,name string) PARTITIONED BY (dt STRING,region STRING);",
                         context).run();
      QueryCommand.parse("alter table " + partitionTable +
                         " add partition (dt='2023', region='china');", context).run();
      QueryCommand.parse("insert into " + partitionTable
                         + " partition (dt='2023', region='china') "
                         + "values ('abc',1),('def',2);", context).run();
      DescribeTableCommand.parse("DESC EXTENDED " + plainTable, context).run();
      DescribeTableCommand.parse("desc extended " + partitionTable, context).run();
      DescribeTableCommand.parse("desc extended " + partitionTable
                                 + " partition (dt='2023', region='china')", context).run();
    } catch (Exception e) {
      ok = false;
      System.out.println("error: " + e.getMessage());
    } finally {
      QueryCommand.parse(deleteTable.apply(plainTable), context).run();
      QueryCommand.parse(deleteTable.apply(partitionTable), context).run();
      if (!ok) {
        Assert.fail();
      }
    }


  }
}
