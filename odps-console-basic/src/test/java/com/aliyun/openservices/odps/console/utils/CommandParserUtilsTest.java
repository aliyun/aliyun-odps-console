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

package com.aliyun.openservices.odps.console.utils;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.CompositeCommand;
import com.aliyun.openservices.odps.console.commands.QueryCommand;
import com.aliyun.openservices.odps.console.commands.ShowTablesCommand;


/**
 * Created by zhenhong.gzh on 2015/6/15.
 */
public class CommandParserUtilsTest {

  String showTbaleCommandText = "show tables;";
  String createTableCommandText = "create table command_test(name string);";
  String negtiveCommandText = "hello odps;";
  String compositeText = showTbaleCommandText + createTableCommandText;


  @Test
  public void commandPaserTest() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    AbstractCommand command;
    command = CommandParserUtils.parseCommand(showTbaleCommandText, ctx);
    assertTrue(command instanceof ShowTablesCommand);

    command = CommandParserUtils.parseCommand(createTableCommandText, ctx);
    assertTrue(command instanceof QueryCommand);

    command = CommandParserUtils.parseCommand(negtiveCommandText, ctx);
    assertTrue(command instanceof QueryCommand);

    command = CommandParserUtils.parseCommand(compositeText, ctx);
    assertTrue(command instanceof CompositeCommand);
    List<AbstractCommand> commandList = ((CompositeCommand) command).getActionList();
    assertTrue(commandList.get(0) instanceof ShowTablesCommand);
    assertTrue(commandList.get(1) instanceof QueryCommand);
  }

  @Test
  public void parsehtmlOptions() throws ODPSConsoleException, OdpsException {
    ExecutionContext ctx = ExecutionContext.init();
    AbstractCommand command;
    String[] st = new String[]{"--html"};
    command = CommandParserUtils.parseOptions(st, ctx);
  }

}
