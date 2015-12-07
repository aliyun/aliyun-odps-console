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

import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/6/2.
 */
public class DescribeInstanceCommandTest {

  private static ExecutionContext context;
  private static Odps odps;
  private static Instance id;

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    context = ExecutionContext.init();
    odps = OdpsConnectionFactory.createOdps(context);
  }

  @Test
  public void testDerscribeInstanceCommand() throws ODPSConsoleException, OdpsException, CloneNotSupportedException {
    id = SQLTask.run(odps, "select count(*) from src;");
    ExecutionContext mycontext = context.clone();
    DescribeInstanceCommand command = DescribeInstanceCommand.parse("desc instance " + id.getId(), mycontext);
    String output = ODPSConsoleUtils.runCommand(command);
    System.out.println(output);
    String result = "ID\\s+\\w+\\s+"
                    + "Owner\\s+.*\\s+"
                    + "StartTime\\s+.*\\s+"
                    + "Status\\s+Running\\s+"
                    + "(AnonymousSQLTask\\s+Running\\s+)?" //missing this field sometimes. delayed.
                    + "Query\\s+select count\\(\\*\\) from src;\\s+";
    Pattern pattern = Pattern.compile(result);
    assertTrue(output, pattern.matcher(output).matches());
  }

  @Test
  public void testDescirbeInstanceTerm() throws OdpsException, ODPSConsoleException {
    id = SQLTask.run(odps, "select count(*) from src;");
    id.waitForSuccess();
    DescribeInstanceCommand command = DescribeInstanceCommand.parse("desc instance " + id.getId(), context);
    String output = ODPSConsoleUtils.runCommand(command);
    System.out.println(output);
    String result = "ID\\s+\\w+\\s+"
                    + "Owner\\s+.*\\s+"
                    + "StartTime\\s+.*\\s+"
                    + "EndTime\\s+.*\\s+"
                    + "Status\\s+Terminated\\s+"
                    + "AnonymousSQLTask\\s+Success\\s+"
                    + "Query\\s+select count\\(\\*\\) from src;\\s+";
    Pattern pattern = Pattern.compile(result);
    assertTrue(output, pattern.matcher(output).matches());
  }

  @Test(expected = ODPSConsoleException.class)
  public void testDescNegative() throws OdpsException, ODPSConsoleException {
    DescribeInstanceCommand command = DescribeInstanceCommand.parse("desc instance NOT_EXIST", context);
    command.run();
  }

  @Test(expected = ODPSConsoleException.class)
  public void testDescInvalid() throws OdpsException, ODPSConsoleException {
    DescribeInstanceCommand command = DescribeInstanceCommand.parse("desc instance INVALIDA INVALIDB", context);
    command.run();
  }

  @Test
  public void testDescCrossProject() throws OdpsException, ODPSConsoleException {
    id = SQLTask.run(odps, "select count(*) from src;");
    DescribeInstanceCommand command = DescribeInstanceCommand.parse("desc instance -p " + odps.getDefaultProject() + " " + id.getId(), context);
    command.run();
  }

  @Test(expected = ODPSConsoleException.class)
  public void testDescCrossProjectNeg() throws OdpsException, ODPSConsoleException {
    id = SQLTask.run(odps, "select count(*) from src;");
    DescribeInstanceCommand command = DescribeInstanceCommand.parse("desc instance -p INVALID " + id.getId(), context);
    command.run();
  }

  @Test(expected = ODPSConsoleException.class)
  public void testDescNoInstance() throws OdpsException, ODPSConsoleException {
    DescribeInstanceCommand command = DescribeInstanceCommand.parse("desc instance", context);
    assertNotNull(command);
    command.run();
  }

}