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

package com.aliyun.openservices.odps.console.commands;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.AfterClass;
import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Created by nizheming on 15/9/1.
 */
public class HtmlModeCommandTest {

  @Test
  public void testHtmlMode() throws ODPSConsoleException, OdpsException {
    ExecutionContext init = ExecutionContext.init();
    HtmlModeCommand command = new HtmlModeCommand("--html", init);
    command.run();
    assertTrue("must be html mode", init.isHtmlMode());
    System.out.println(new File("html").getAbsolutePath());
    assertTrue(new File("html").isDirectory());
  }

  @AfterClass
  public static void cleanup() {
    File f = new File("html");
    if (f.exists()) {
      f.delete();
    }
  }
}