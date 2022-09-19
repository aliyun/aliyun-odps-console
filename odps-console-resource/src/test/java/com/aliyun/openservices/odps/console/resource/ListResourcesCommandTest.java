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
public class ListResourcesCommandTest {

  private static String[] positives = {
      "LIST RESOURCES",
      "list Resources",
      " list resources \r",
      "ls resources",
      "LS RESOURCES",
      "ls resources",
      "\n\r\t List\tresources   \n\r\t",
      "\n\r\t ls\tresources   \n\r\t"};

  private static String[] negatives = {
      "list",
      "LIST",
      "list resource",
      "ls resource",
      "ls resources_xxx"};

  private static String[] matchProjects = {
      "ls resources in project_name",
      "\n\r\tLs RESOURCES \t IN project_name\t\t",
      "ls resources in project_name.schema_name\n\r\t",
      "SHOW RESOURCES IN project_name.schema_name"};

  @Test
  public void testMatchPositive() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    ListResourcesCommand test;
    for (String cmd : positives) {
      test = ListResourcesCommand.parse(cmd, ctx);
      Assert.assertNotNull(test);
    }
  }

  @Test
  public void testMatchNegative() throws OdpsException, ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();

    for (String cmd : negatives) {
      Assert.assertNull(ListResourcesCommand.parse(cmd, ctx));
    }
  }

  @Test
  public void testRightCommand() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();

    ListResourcesCommand test;
    for (String cmd : matchProjects) {
      test = ListResourcesCommand.parse(cmd, ctx);
      Assert.assertNotNull(test);
    }
  }
}
