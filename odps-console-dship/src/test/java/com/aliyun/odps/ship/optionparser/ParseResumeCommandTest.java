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
 * 测试resume命令解析
 * */
public class ParseResumeCommandTest {

  /**
   * 正常解析resume命令
   * */
  @Test
  public void testResumeCommand() throws Exception {
    String[] args;

    args = new String[] {"resume", "sidxxx"};
    OptionsBuilder.buildResumeOption(args);
    String sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    assertEquals("sid not equal", "sidxxx", sid);

    args = new String[] {"resume"};
    OptionsBuilder.buildResumeOption(args);
    sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    assertNull("sid not null", sid);

  }

  /**
   * 测试resume命令，指定force选项
   * */
  @Test
  public void testResumeCommandForce() throws Exception {
    String[] args;

    args = new String[] {"resume", "sidxxx", "-force"};
    OptionsBuilder.buildResumeOption(args);
    String sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    assertEquals("sid not equal", "sidxxx", sid);
    assertEquals("force not equal", "true", DshipContext.INSTANCE.get("resume-force"));


    args = new String[] {"resume", "-force"};
    OptionsBuilder.buildResumeOption(args);
    sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    assertEquals("sid not equal", null, sid);
    assertEquals("force not equal", "true", DshipContext.INSTANCE.get("resume-force"));

    args = new String[] {"resume", "-f"};
    OptionsBuilder.buildResumeOption(args);
    sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    assertEquals("sid not null", null, sid);
    assertEquals("force not equal", "true", DshipContext.INSTANCE.get("resume-force"));

    args = new String[] {"resume", "sidxxx"};
    OptionsBuilder.buildResumeOption(args);
    sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    assertEquals("sid not equal", "sidxxx", sid);
    assertEquals("force not equal", null, DshipContext.INSTANCE.get("resume-force"));

    args = new String[] {"resume"};
    OptionsBuilder.buildResumeOption(args);
    sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);
    assertEquals("sid not null", null, sid);
    assertEquals("force not equal", null, DshipContext.INSTANCE.get("resume-force"));

  }

  /**
   * 测试resume命令解析出错，出错符合预期
   * */
  @Test
  public void testResumeCommandFail() throws Exception {
    String[] args;
    try {
      args = new String[] {"resume", "sidxxx", "sid2"};
      OptionsBuilder.buildResumeOption(args);
      fail("need fail.");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unknown command") >= 0);
    }

    try {
      args = new String[] {"resume", "sidxxx", "-key1=value2"};
      OptionsBuilder.buildResumeOption(args);
      fail("need fail.");
    } catch (Exception e) {
      assertTrue("need include message.", e.getMessage().indexOf("Unrecognized") >= 0);
    }

  }

}
