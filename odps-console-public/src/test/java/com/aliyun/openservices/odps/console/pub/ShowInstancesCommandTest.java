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
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Created by zhenhong.gzh on 2015/4/20.
 */
public class ShowInstancesCommandTest {

  private static String[] positives = {"SHOW P", "show P", " SHOW \n\r PROCESSLIST \r",
                                       "show Proc", "\n\r\t SHow\tInstances\n\r\t",
                                       "Ls\t Instances\n\r\t",  "LisT\t Instances\n\r\t",
                                       "list\t Instances -all", "show instances \n\t\r -all 100"};

  private static String[] negatives = {"show", "show instance", "show Pro", "show tables"};

  private static String[]
      bad_paras =
      {"ls instances -p -test -ls -error", "ls instances -limit", "ls instances test_project 100",
       "ls instances -limit sdf100", "show instances abc456", "show instances fron",
       "show instances from 20150411", "show instances from 20140411 to 20150416",
       "ls instances -limit -1", "show instances -all yes", "show instances -a"};

  private static String[]
      right_paras =
      {"ls instances -limit 2 -p project_name", "lisT instances -limit 2 -p project_name",
       "ls instances -limit 4", "show instances 2",
       "show instances From 2014-04-15 tO 2015-04-16 3",
       "show instances From 2014-04-15 tO 2015-04-16 3 -all", "show instances 30 -all"};

  @Test
  public void testMatchPositive() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    ShowInstanceCommand test;
    for (String cmd : positives) {
      test = ShowInstanceCommand.parse(cmd, ctx);
      Assert.assertNotNull(test);
      test.run();
    }
  }


  @Test
  public void testMatchNegative() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    ShowInstanceCommand test;

    for (String cmd : negatives) {
      Assert.assertNull(ShowInstanceCommand.parse(cmd, ctx));
    }
  }

  @Test
  public void testLackParas() throws ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();

    for (String cmd : bad_paras) {
      try {
        ShowInstanceCommand.parse(cmd, context);
        Assert.assertTrue("Current command: " + cmd, false);
      } catch (ODPSConsoleException e) {
      }
    }
  }

  @Test
  public void testRightCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();
    String project = ctx.getProjectName();

    ShowInstanceCommand test;
    for (String cmd : right_paras) {
      test = ShowInstanceCommand.parse(cmd.replace("project_name", project), ctx);
      Assert.assertNotNull(test);
      test.run();
    }
  }
}
