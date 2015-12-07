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

package com.aliyun.odps.ship.optionparser;

import static org.junit.Assert.*;

import org.junit.Test;

import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;

/**
 * 测试purge命令
 * */
public class ParsePurgeCommandTest {

  /**
   * 测试purge命令项的解析
   * */
  @Test
  public void testPurgeCommand() throws Exception {

    String[] args;

    args = new String[] {"purge", "5"};
    OptionsBuilder.buildPurgeOption(args);
    String number = DshipContext.INSTANCE.get(Constants.PURGE_NUMBER);
    assertEquals("number not equal", "5", number);

    args = new String[] {"purge"};
    OptionsBuilder.buildPurgeOption(args);
    number = DshipContext.INSTANCE.get(Constants.PURGE_NUMBER);
    assertEquals("number not equal", "3", number);

  }

  /**
   * 测试purge命令项解析出错，出错信息符合预期
   * */
  @Test
  public void testPurgeFail() {
    String[] args;
    try {
      args = new String[] {"purge", "Bad"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

    try {
      args = new String[] {"purge", "5", "5"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

    try {
      args = new String[] {"purge", "-op=xxx"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unrecognized") >= 0);
    }
  }

}
