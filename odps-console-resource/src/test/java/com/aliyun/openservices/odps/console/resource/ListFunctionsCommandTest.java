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

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;


/**
 * Created by zhenhong.gzh on 2015/4/21.
 */
public class ListFunctionsCommandTest {

  private static String[]
      positives =
      {"LIST FUNCTIONS", "list Functions", " list functions \r",
       "ls \n\r\t functions", "LS FUNCTIONS", "\n\r\t List\tfunctions   \n\r\t",
       "\n\r\t ls\tfunctions   \n\r\t"};

  private static String[]
      negatives =
      {"list", "LIST", "show functions", "list function", "ls function",
       "list functions -p project_name"};

  private static String[]
      matchProjects =
      {"ls functions -p project_name", "\n\r\tLs FUNCTIONS \t -p project_name\t\t",
       "ls functions -p=project_name\n\r\t"};

  private static String[]
      badPara =
      {"ls functions -p", "ls functions -project", "ls functions -test project_name",
       "ls functions -p project_name -test"};

  @Test
  public void testMatchPositive() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    ListFunctionsCommand test;
    for (String cmd : positives) {
      test = ListFunctionsCommand.parse(cmd, ctx);
      Assert.assertNotNull(test);
      test.run();
    }
  }


  @Test
  public void testMatchNegative() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();

    for (String cmd : negatives) {
      //debug
      System.out.println(cmd);
      //debug end
      Assert.assertNull(ListFunctionsCommand.parse(cmd, ctx));
    }
  }

  @Test(expected = ODPSConsoleException.class)
  public void testLackParas() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : badPara) {
      ListFunctionsCommand.parse(cmd, context);
    }
  }

  @Test
  public void testRightCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();
    String project = ctx.getProjectName();

    ListFunctionsCommand test;
    for (String cmd : matchProjects) {
      test = ListFunctionsCommand.parse(cmd.replace("project_name", project), ctx);
      System.out.println(cmd);

      Assert.assertNotNull(test);
      test.run();
    }
  }
}
