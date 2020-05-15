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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/4/20.
 */
public class DescribeResourceCommandTest {

  private static ExecutionContext context;
  private static Odps odps;
  private static final String CONFIG_FILE = "odps_config.ini";

  @BeforeClass
  public static void setup() throws ODPSConsoleException, FileNotFoundException, OdpsException {
    context = ExecutionContext.init();
    odps = OdpsConnectionFactory.createOdps(context);
    FileResource r = new FileResource();
    r.setName(CONFIG_FILE);
    InputStream in = new FileInputStream(ODPSConsoleUtils.getConfigFilePath());
    if (odps.resources().exists(CONFIG_FILE)) {
      odps.resources().update(r, in);
    } else {
      odps.resources().create(r, in);
    }
  }

  private String[] positives = new String[]{
      "desc resource odps_config.ini",
      "DESC RESOURCE odps_config.ini",
      "DESC\nresource \t odps_config.ini",
      "desc resource *:odps_config.ini",
      "desc resource -p * odps_config.ini",
      "desc resource -p * *:odps_config.ini",
  };

  @Test
  public void postive() throws ODPSConsoleException, OdpsException {
    for (String cmd : positives) {
      cmd = cmd.replaceAll("\\*", odps.getDefaultProject());
      System.out.println(cmd);
      AbstractCommand command = DescribeResourceCommand.parse(cmd, context);
      assertTrue(cmd, command instanceof DescribeResourceCommand);
      command.execute();
    }
  }

  private String[] negatives = new String[]{
      "DESC resource ",
      "Desc resource  -p aaaa",
      "desc resource a b c",
      "desc resource -p aaaa *:odps_config.ini",
  };

  @Test
  public void negative() throws Exception {
    int count = 0;
    for (String cmd : negatives) {
      cmd = cmd.replaceAll("\\*", odps.getDefaultProject());
      System.out.println(cmd);
      try {
        AbstractCommand command = DescribeResourceCommand.parse(cmd, context);
      } catch (ODPSConsoleException e) {
        count ++;
      }
    }
    assertEquals(negatives.length, count);
  }

}
