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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

// TODO: add UT cases for project.schema.table
public class ShowPartitionsCommandTest {

  private static final String[] positives = {
      "ls partitions -p p1 t1",
      " SHOW PARTITIONS projectname.tablename",
      "show partitions p.t",
      "show partitions t",
      "list partitions p.t",
      "list partitions p.t",
      "list partitions -p p p.t",
      "\r\t\nShoW\t\rPartitions\n\tprojectname.tablename\r\t\n",
      " LS PARTITIONS projectname.tablename",
      "\r\t\nLs\t\rPartitions\n\tprojectname.tablename\r\t\n",
      "LS pArtitions -p projectname tablename",
      "\r\t\nLisT\t\rPartitions\n\tprojectname.tablename\r\t\n"};

  private static final String[] negatives = {
      "SHOW", "show tables",
      "show PARTITION xxx", "ls PARTITION xxx",
      "list PARTITION xxx", "LS",
      "ls tables", "LIST",
      "LIst tables",
      };

  private static final String[] error = {
      "SHOW PARTITIONS ",
      "LS PARTITIONS ",
      "show partitions table parttiin('a=b')",
      "LS pArtitions -p projectname",
      "ls partitions -p projectname.tablename",
      "list partitions -p projectname.tablename"
  };

  @Test
  public void testPositive() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : positives) {
      System.out.println(cmd);
      assertNotNull(ShowPartitionsCommand.parse(cmd, ctx));
    }
  }

  private static final Pattern TEST_PATTERN =
      Pattern.compile("\\s*(LS|LIST)\\s+PARTITIONS\\s+(.*)",
                      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  @Test
  public void testNegative() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : negatives) {
      System.out.println(cmd);
      assertNull(ShowPartitionsCommand.parse(cmd, ctx));
    }
  }

  @Test
  public void testError() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    for (String cmd : error) {
      try {
        ShowPartitionsCommand.parse(cmd, ctx);
        Assert.fail("should throw exception");
      } catch (ODPSConsoleException e) {
        // e.printStackTrace();
        // expected
      }
    }
  }

}
