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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/8/24.
 */
public class DescribeFunctionCommandTest {

  public static final String CONSOLE_INI = "console.ini";

  @Before
  public void setup() throws ODPSConsoleException, OdpsException, FileNotFoundException {
    ExecutionContext init = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(init);
    if (!odps.functions().exists("console_lower")) {
      Function f = new Function();
      f.setName("console_lower");
      f.setClassPath("console.lower");
      FileResource r = new FileResource();
      r.setName(CONSOLE_INI);
      InputStream in = new FileInputStream(ODPSConsoleUtils.getConfigFilePath());
      if (odps.resources().exists(CONSOLE_INI)) {
        odps.resources().update(r, in);
      } else {
        odps.resources().create(r, in);
      }
      ArrayList<String> list = new ArrayList<String>();
      list.add(CONSOLE_INI);
      f.setResources(list);
      odps.functions().create(f);
    }
  }

  @Test
  public void testDescFunc() throws ODPSConsoleException, OdpsException {
    ExecutionContext init = ExecutionContext.init();
    AbstractCommand cmd = DescribeFunctionCommand.parse("desc function a", init);
    Assert.assertNotNull(cmd);
  }

  @Test
  public void testDescFuncNegative() throws ODPSConsoleException {
    ExecutionContext init = ExecutionContext.init();
    String[] negative = new String[]{
      "desc function  ",
      "desc function .",
      "desc function a.",
      "desc function a..b",
      "desc function a.b.c.d"
    };
    for (String cmd: negative) {
      System.out.println(cmd);
      try{
        AbstractCommand c = DescribeFunctionCommand.parse(cmd, init);
      } catch (Exception e) {
        continue;
      }
      Assert.fail();
    }
  }
}
