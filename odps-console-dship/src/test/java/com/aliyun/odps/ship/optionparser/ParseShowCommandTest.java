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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;

/**
 * 测试show 命令的解析
 * */
public class ParseShowCommandTest {


  /**
   * 测试show命令正常执行
   * */
  @Test
  public void testShowCommand() throws Exception {

    String[] args;

    args = new String[] {"show", "history"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "history", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));

    args = new String[] {"show", "history", "-number=5"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "history", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertEquals("number not equal", "5", DshipContext.INSTANCE.get("number"));

    args = new String[] {"show", "history", "-number=6"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "history", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertEquals("number not equal", "6", DshipContext.INSTANCE.get("number"));

    args = new String[] {"show", "log", "sidxxx"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "log", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertEquals("sid not equal", "sidxxx", DshipContext.INSTANCE.get(Constants.SESSION_ID));

    args = new String[] {"show", "log"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "log", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertNull("sid not null", DshipContext.INSTANCE.get(Constants.SESSION_ID));

    args = new String[] {"show", "bad", "sidxxx"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "bad", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertEquals("sid not equal", "sidxxx", DshipContext.INSTANCE.get(Constants.SESSION_ID));

    args = new String[] {"show", "bad"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "bad", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertNull("sid not null", DshipContext.INSTANCE.get(Constants.SESSION_ID));
  }

  /**
   * 测试show命令短命令格式的执行
   * */
  @Test
  public void testShowCommandShort() throws Exception {

    String[] args;

    args = new String[] {"show", "h"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "history", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));

    args = new String[] {"show", "h", "-number=5"};
     OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "history", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertEquals("number not equal", "5", DshipContext.INSTANCE.get("number"));

    args = new String[] {"show", "h", "-number=6"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "history", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertEquals("number not equal", "6", DshipContext.INSTANCE.get("number"));

    args = new String[] {"show", "l", "sidxxx"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "log", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertEquals("sid not equal", "sidxxx", DshipContext.INSTANCE.get(Constants.SESSION_ID));

    args = new String[] {"show", "l"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "log", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertNull("sid not null", DshipContext.INSTANCE.get(Constants.SESSION_ID));


    args = new String[] {"show", "b", "sidxxx"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "bad", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertEquals("sid not equal", "sidxxx", DshipContext.INSTANCE.get(Constants.SESSION_ID));

    args = new String[] {"show", "b"};
    OptionsBuilder.buildShowOption(args);
    assertEquals("cmd not equal", "bad", DshipContext.INSTANCE.get(Constants.SHOW_COMMAND));
    assertNull("sid not null", DshipContext.INSTANCE.get(Constants.SESSION_ID));

  }

  /**
   * 测试执行show history命令失败，异常信息符合预期
   * */
  @Test
  public void testShowHistoryCommandFail() throws Exception {

    String[] args;

    try {
      args = new String[] {"show", "history", "-number=a"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {

      assertTrue("need include message.",
          e.getMessage().indexOf("Illegal number\nType 'tunnel help show' for usage.") >= 0);
    }

    try {
      args = new String[] {"show", "history1"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

    try {
      args = new String[] {"show", "history", "-sss=5"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unrecognized") >= 0);
    }

    try {
      args = new String[] {"show", "history", "sidxxx"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

  }

  /**
   * 测试执行show log\show bad命令失败，异常信息符合预期
   * */
  @Test
  public void testShowLogBadCommandFail() throws Exception {

    String[] args;

    try {
      args = new String[] {"show", "log", "-number=5"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

    try {
      args = new String[] {"show", "bad", "-number=5"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

    try {
      args = new String[] {"show", "Bad"};
      OptionsBuilder.buildShowOption(args);
      fail("need fail");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }
  }
}
