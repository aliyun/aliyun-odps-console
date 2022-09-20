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

import static org.junit.Assert.*;

import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;

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
      "\r\t\n  DesCribe\r\n\tproject_name.table_name\r\t\nPARTITION\t\n(pt='1', dt='1')\t\n" };

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
}
