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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsole;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class ShowPartitionsCommandTest {

  private static final String[] positives = {" SHOW PARTITIONS projectname.tablename",
                                             "\r\t\nShoW\t\rPartitions\n\tprojectname.tablename\r\t\n",
                                             " LS PARTITIONS projectname.tablename",
                                             "\r\t\nLs\t\rPartitions\n\tprojectname.tablename\r\t\n",
                                             "LS pArtitions -p projectname tablename",
                                             "\r\t\nLisT\t\rPartitions\n\tprojectname.tablename\r\t\n"};

  private static final String[] negatives = {"SHOW", "show tables",
                                             "show PARTITION xxx", "ls PARTITION xxx",
                                             "list PARTITION xxx", "LS",
                                             "ls tables", "LIST",
                                             "LIst tables"};

  private static final String[]
      exceptions =
      {"SHOW PARTITIONS ", "LS PARTITIONS ", "LS pArtitions -p projectname",
       "ls partitions -p projectname.tablename",
       "list partitions -p projectname.tablename"};

  @Test
  public void testPositive() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : positives) {
      assertNotNull(ShowPartitionsCommand.parse(cmd, ctx));
    }
  }

  private static final Pattern TEST_PATTERN =
      Pattern.compile("\\s*(LS|LIST)\\s+PARTITIONS\\s+(.*)",
                      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  @Test
  public void testTablePart() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();

    String[]
        command =
        {"ls partitions -p projectname tablename(pt=\"1234\",at=\"245\")",
         "ls partitions projectname.tablename(pt=\"1234\",at=\"245\")",
         "list partitions projectname.tablename(pt=\"1234\",at=\"245\")"};

    for (String cmd : command) {
      ShowPartitionsCommand test = ShowPartitionsCommand.parse(cmd, ctx);
      assertNotNull(test);

      Matcher pubMatcher = TEST_PATTERN.matcher(cmd);
      assertTrue(pubMatcher.matches());
      ODPSConsoleUtils.TablePart tablePart = ShowPartitionsCommand.getTablePartFromPublicCommand(
          pubMatcher.group(2));
      assertNotNull(tablePart);
      assertNotNull(tablePart.tableName);
      String[] tableSpec = ODPSConsoleUtils.parseTableSpec(tablePart.tableName);
      String project = tableSpec[0];
      String table = tableSpec[1];

      assertEquals(project, "projectname");
      assertEquals(table, "tablename");
      assertEquals(tablePart.partitionSpec, "pt=\"1234\",at=\"245\"");
    }
  }

  @Test
  public void testNegative() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : negatives) {
      assertNull(ShowPartitionsCommand.parse(cmd, ctx));
    }
  }

  @Test
  public void testException() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : exceptions) {
      try {
        ShowPartitionsCommand.parse(cmd, ctx);
        Assert.assertNotNull(null);
      } catch (ODPSConsoleException e) {

      }
    }
  }
}
