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

package com.aliyun.openservices.odps.console.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
/**
 * Created by zhenhong.gzh on 2015/6/15.
 */
public class PluginUtilTest {

  public String commandString1 = "command_test2:100,command_test1:3000,command_test5:-100,command_test3:0,command_test4:-1";
  public String commandString2 = "command_testMax:" + PluginPriorityCommand.MAX_PRIORITY + ",command_test,command_testMin:" + PluginPriorityCommand.MIN_PRIORITY;
  public String commandString3 = "command_test:error";

  @Test
  public void normalTest() throws Exception {
    List<PluginPriorityCommand> commandList = new ArrayList<PluginPriorityCommand>();

    PluginUtil.getPriorityCommandFromString(commandList, commandString1);
    Collections.sort(commandList);

    int i = 0;
    for (PluginPriorityCommand command : commandList) {
      i++;

      Assert.assertEquals(command.getCommandName(), "command_test" + i);
    }
  }

  @Test
  public void boundAndDefalutTest() throws Exception {
    List<PluginPriorityCommand> commandList = new ArrayList<PluginPriorityCommand>();

    PluginUtil.getPriorityCommandFromString(commandList, commandString2);
    Collections.sort(commandList);

    int i = 0;
    Assert.assertEquals("command_testMax", (commandList.get(i)).getCommandName());
    Assert.assertTrue(PluginPriorityCommand.MAX_PRIORITY == (commandList.get(i).getCommandPriority()));

    i++;

    Assert.assertEquals("command_test", (commandList.get(i)).getCommandName());
    Assert.assertTrue(0 == (commandList.get(i).getCommandPriority()));

    i++;

    Assert.assertEquals("command_testMin", (commandList.get(i)).getCommandName());
    Assert.assertTrue(PluginPriorityCommand.MIN_PRIORITY == (commandList.get(i).getCommandPriority()));

    commandList.add(new PluginPriorityCommand("command_test2", 20));
    Collections.sort(commandList);

    Assert.assertEquals("command_test2", (commandList.get(1)).getCommandName());
    Assert.assertTrue(20 == (commandList.get(1).getCommandPriority()));

  }

  @Test(expected = Exception.class)
  public void priorityTypeErrorTest() throws Exception {
    List<PluginPriorityCommand> commandList = new ArrayList<PluginPriorityCommand>();

    PluginUtil.getPriorityCommandFromString(commandList, commandString3);
  }
}
