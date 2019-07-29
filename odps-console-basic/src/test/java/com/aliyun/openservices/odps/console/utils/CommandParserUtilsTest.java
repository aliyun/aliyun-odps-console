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

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.CompositeCommand;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


/**
 * Created by zhenhong.gzh on 2015/6/15.
 */
public class CommandParserUtilsTest {

  String showTbaleCommandText = "show tables;";
  String createTableCommandText = "create table command_test(name string);";

  @Test
  public void commandPaserTest() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    AbstractCommand command;
    command = CommandParserUtils.parseCommand(showTbaleCommandText, ctx);
    assertTrue(command instanceof CompositeCommand);
  }

  @Test
  public void testExtendedList() throws ODPSConsoleException {
    ExecutionContext ctx = ExecutionContext.init();
    int beforeSize = CommandParserUtils.getExtendedCommandList().size();
    CommandParserUtils.parseCommand(createTableCommandText, ctx);
    CommandParserUtils.parseCommand(createTableCommandText, ctx);
    CommandParserUtils.parseCommand(createTableCommandText, ctx);
    CommandParserUtils.parseCommand(createTableCommandText, ctx);
    int afterSize = CommandParserUtils.getExtendedCommandList().size();
    assertEquals(beforeSize, afterSize);
  }

  @Test
  public void testGetCommandArgs() throws ODPSConsoleException {
    // parse negative
    String negative[][] = {{"-G", "23"}, {"-G"}, {"23"}, {"-I"}, {"-i"}, {"-I ", "345", "-e"}, {}};
    for (String[] neg : negative) {
      assertArrayEquals(neg, CommandParserUtils.getCommandArgs(neg));
    }

    // parse exception
    String invalidFd[][] = {{"-I ", "A23"}, {"-i \t", "cbd"}};
    int count = 0;
    for (String[] invalid : invalidFd) {
      try {
        CommandParserUtils.getCommandArgs(invalid);
      } catch (ODPSConsoleException e) {
        count++;
        assertTrue(e.getCause() instanceof FileNotFoundException);
      }
    }
    assertEquals(count, invalidFd.length);

    // positive case
    File file = new File("args_test_file");
    if (file.exists()) {
      file.delete();
    }

    try {
      if (!file.createNewFile()) {
        throw new IOException();
      }
      // test empty
      String args[] = {"-I", file.getPath()};
      assertEquals(0, CommandParserUtils.getCommandArgs(args).length);

      // test args
      String expect [] =
          {"-p", "user", "-u", "password", "--endpoint=http://test",
           "--project=test_project", "-e", "select * from dual;\n create table tt(name string); \r\n"};

      FileWriter writer = new FileWriter(file);
      for (String word : expect) {
        writer.write(" ");
        writer.write("\0");
        writer.write(word);
        writer.write("\0");
      }
      writer.close();

      String args1[] = {"-I", file.getPath()};
      assertArrayEquals(expect, CommandParserUtils.getCommandArgs(args1));
      // getArgsFromFileDescriptor() has close the fd, do not need here

    } catch (IOException e) {
      fail(e.getMessage());
    }

  }
}
