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

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

/**
 * Created by nizheming on 15/6/19.
 */
public class UseProjectCommandTest {

  @Test
  public void testHttps() throws ODPSConsoleException, OdpsException, IOException {
    ExecutionContext ctx = ExecutionContext.init();
    Properties properties = new Properties();
    properties.load(new FileInputStream(ODPSConsoleUtils.getConfigFilePath()));
    String endpoint = (String) properties.get("https_end_point");
    ctx.setEndpoint(endpoint);
    UseProjectCommand command = UseProjectCommand.parse("use " + ctx.getProjectName(), ctx);
    command.run();
    // String result = command.getMsg();
    // assertEquals(result,"WARNING: untrusted https connection:'" + endpoint + "', add https_check=true in config file to avoid this warning.");
  }

  @Test(expected = RuntimeException.class)
  public void testHttpsNeg() throws ODPSConsoleException, OdpsException, IOException {
    ExecutionContext ctx = ExecutionContext.init();
    ctx.setHttpsCheck(true);
    Properties properties = new Properties();
    properties.load(new FileInputStream(ODPSConsoleUtils.getConfigFilePath()));
    String endpoint = (String) properties.get("https_end_point");
    ctx.setEndpoint(endpoint);
    UseProjectCommand command = UseProjectCommand.parse("use " + ctx.getProjectName(), ctx);
    command.run();
    // String result = command.getMsg();
    // assertEquals(result,"WARNING: untrusted https connection:'" + endpoint + "', add https_check=true in config file to avoid this warning.");
  }
}