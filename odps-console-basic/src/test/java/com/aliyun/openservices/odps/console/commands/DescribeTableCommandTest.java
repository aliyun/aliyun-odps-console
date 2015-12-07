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

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils.TablePart;

public class DescribeTableCommandTest {

  private static final String[] positives = { "DESCRIBE project_name.table_name",
      "DESC project_name.table_name", "desc project_name.table_name",
      "\r\n\tDesCribe\r\n\tproject_name.table_name\r\t\n" };

  private static final String[] negatives = { "DESCRIBE", "DESC", "DESC ",
      "DESC project_name.table_name PARTITIONS (pt='x', dt='x')",
      };

  private static final String[] tablePart_postive = {
      "project_name.table_name PARTITION(pt='1', dt='1')",
      "project_name.table_name PARTITION(pt='1', dt='1')",
      "project_name.table_name partition(pt='1', dt='1')",
      "project_name.table_name\r\t\nPARTITION\t\n(pt='1', dt='1')\t\n" };

  private static final String[] p_positives = {
      "DESCRIBE project_name.table_name PARTITION(pt='1', dt='1')",
      "DESC project_name.table_name PARTITION(pt='1', dt='1')",
      "desc project_name.table_name partition(pt='1', dt='1')",
      "desc project_name.table_name partition(pt=\"1\", dt=\"1\")",
      "desc project_name.table_name partition(pt='1', dt=\"1\")",
      "\r\t\n  DesCribe\r\n\tproject_name.table_name\r\t\nPARTITION\t\n(pt='1', dt='1')\t\n" };

  @Test
  public void testPartitionParse() {
    TablePart tablePart = ODPSConsoleUtils.getTablePart("table_name partition(pt='1', dt='1')");
    Assert.assertEquals("table_name", tablePart.tableName);
    Assert.assertEquals("pt='1', dt='1'", tablePart.partitionSpec);

    for (String cmd : tablePart_postive) {
      tablePart = ODPSConsoleUtils.getTablePart(cmd);
      Assert.assertEquals("project_name.table_name", tablePart.tableName);
      Assert.assertEquals("pt='1', dt='1'", tablePart.partitionSpec);
    }
  }

  @Test
  public void testCommandParse() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : p_positives) {
      AbstractCommand command = DescribeCommand.parse(cmd, context);
      assertNotNull(command);
      assertTrue(command instanceof DescribeTableCommand);
    }
    
    for (String cmd : positives) {
      AbstractCommand command = DescribeCommand.parse(cmd, context);
      assertNotNull(command);
      assertTrue(command instanceof DescribeTableCommand);
    }
  }
  
  @Test
  public void testCommandNeagetive() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    for (String cmd : negatives) {
      AbstractCommand command = DescribeCommand.parse(cmd, context);
      assertNull(command);
    }
  }
}
