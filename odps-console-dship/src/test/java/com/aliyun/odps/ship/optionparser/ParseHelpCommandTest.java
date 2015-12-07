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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import com.aliyun.odps.ship.DShip;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;

/**
 * 测试help命令的解析及执行
 * */
public class ParseHelpCommandTest {

  /**
   * 测试help命令的解析，判断解析后的命令项
   * */
  @Test
  public void testHelpCommand() throws Exception {
    String[] args;

    args = new String[] {"help", "upload"};
    OptionsBuilder.buildHelpOption(args);
    String cmd = DshipContext.INSTANCE.get(Constants.HELP_SUBCOMMAND);
    assertEquals("cmd not equal", "upload", cmd);

    args = new String[] {"help"};
    OptionsBuilder.buildHelpOption(args);
    cmd = DshipContext.INSTANCE.get(Constants.HELP_SUBCOMMAND);
    assertNull("cmd not null", cmd);

  }

  /**
   * 测试不存在的命令的帮助，出错信息符合预期
   * */
  @Test
  public void testHelpCommandFail() throws Exception {
    String[] args;
    try {
      args = new String[] {"help", "upload", "download"};
      OptionsBuilder.buildHelpOption(args);
      fail("need fail.");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

    try {
      args = new String[] {"help", "upload1"};
      OptionsBuilder.buildHelpOption(args);
      fail("need fail.");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

    try {
      args = new String[] {"help", "-key1=value2"};
      OptionsBuilder.buildHelpOption(args);
      fail("need fail.");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unrecognized") >= 0);
    }
  }

  /**
   * 测试执行help命令正常
   * */
  @Test
  public void testShowHelp() throws Exception {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream s = new PrintStream(out);
    PrintStream old = System.out;
    System.setOut(s);

    String[] args = new String[] {"help", "upload"};
    DShip.parseSubCommand(args);
    args = new String[] {"help", "download"};
    DShip.parseSubCommand(args);
    args = new String[] {"help", "resume"};
    DShip.parseSubCommand(args);
    args = new String[] {"help", "show"};
    DShip.parseSubCommand(args);
    args = new String[] {"help", "config"};
    DShip.parseSubCommand(args);
    args = new String[] {"help", "purge"};
    DShip.parseSubCommand(args);
    args = new String[] {"help", "help"};
    DShip.parseSubCommand(args);
    args = new String[] {"help"};
    DShip.parseSubCommand(args);

    System.setOut(old);
  }
}
