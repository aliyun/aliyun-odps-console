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

package com.aliyun.openservices.odps.console.pub;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/4/16.
 */
public class DescribeProjectCommandTest {

  @Test
  public void testDescribeProjectCommand()
      throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    AbstractCommand
        command =
        DescribeProjectCommand.parse("Desc project " + odps.getDefaultProject(), context);
    assertTrue(command instanceof DescribeProjectCommand);
    command.run();
  }

  @Test(expected = ODPSConsoleException.class)
  public void testDescribeProjectCommandNeg()
      throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    AbstractCommand command = DescribeProjectCommand.parse("Desc project", context);
  }

  @Test
  public void testDescribeProjectExtendedCommand()
      throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    AbstractCommand
        command =
        DescribeProjectCommand.parse("Desc project -extended " + odps.getDefaultProject(), context);
    assertTrue(command instanceof DescribeProjectCommand);
    command.run();
  }

  @Test
  public void testDescribeProjectExtendedAfterCommand()
      throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    AbstractCommand
        command =
        DescribeProjectCommand.parse("Desc project " + odps.getDefaultProject() + " -extended ", context);
    assertTrue(command instanceof DescribeProjectCommand);
    command.run();
  }

  @Test(expected = ODPSConsoleException.class)
  public void testDescribeProjectExtendedNegCommand()
      throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    AbstractCommand
        command =
        DescribeProjectCommand.parse("Desc project " + odps.getDefaultProject() + " -abcd ", context);
  }

}
