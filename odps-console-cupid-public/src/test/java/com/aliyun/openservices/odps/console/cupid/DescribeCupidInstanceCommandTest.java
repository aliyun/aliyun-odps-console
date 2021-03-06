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

package com.aliyun.openservices.odps.console.cupid;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class DescribeCupidInstanceCommandTest {

  @Test
  public void testParse() {
    ExecutionContext cxt = new ExecutionContext();

    /**
     * 测试是否接受该命令
     */
    //negative, 可能发生歧义的命令
    String[] cmds = {"describe instance", "desc resource",
                     "desc onlinemodel", "desc project", "desc shard"};
    DescribeCupidInstanceCommand result;
    for (String cmd : cmds) {
      try {
        result = DescribeCupidInstanceCommand.parse(cmd, cxt);
        Assert.assertNull(result);
      } catch (ODPSConsoleException e) {
        e.printStackTrace();
        System.err.println("should be null. ");
      }
    }

    //positive
    cmds = new String[]{"describe cupid instance -i 123"};
    result = null;
    for (String cmd : cmds) {
      try {
        result = DescribeCupidInstanceCommand.parse(cmd, cxt);
      } catch (ODPSConsoleException e) {
        e.printStackTrace();
      }
      Assert.assertNotNull(result);
    }

    /**
     * 测试是否解析参数成功
     */
    //negative
    cmds = new String[]{"describe cupid instance", "describe cupid instance -i 123 -n name"};
    //don't support two consecutive options likes "command -i -t"
    for (String cmd : cmds) {
      try {
        DescribeCupidInstanceCommand.parse(cmd, cxt);
        Assert.fail(cmd + " failed.");
      } catch (ODPSConsoleException e) {
      }
    }

    //positive
    result = null;
    try {
      String cmd = "describe cupid instance -i 123";
      result = DescribeCupidInstanceCommand.parse(cmd, cxt);
    } catch (ODPSConsoleException e) {
      e.printStackTrace();
    }
    assert result != null;
    Assert.assertEquals("123", result.getInstanceID());

    System.out.println("Pass all tests. ");
  }
}
