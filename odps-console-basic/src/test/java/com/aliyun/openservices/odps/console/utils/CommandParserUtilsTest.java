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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
  public void testGetCommandArgsReadsAndDeletesRegularFile() throws Exception {
    Path argsFile = Files.createTempFile("odpscmd-args-", ".tmp");
    String[] expected =
        {"-p", "test_project", "-e", "select * from dual;\nselect 1;"};

    try {
      Files.write(argsFile,
          (String.join("\0", expected) + "\0").getBytes(StandardCharsets.UTF_8));

      assertArrayEquals(expected,
          CommandParserUtils.getCommandArgs(new String[]{"-I", argsFile.toString()}));
      assertFalse(Files.exists(argsFile));
    } finally {
      Files.deleteIfExists(argsFile);
    }
  }

  @Test
  public void testGetCommandArgsRejectsEmptyPath() {
    try {
      CommandParserUtils.getCommandArgs(new String[]{"-I", ""});
      fail("empty args file path should be rejected");
    } catch (ODPSConsoleException e) {
      assertTrue(e.getMessage().contains("args file path is empty"));
    }
  }

  @Test
  public void testGetCommandArgsDoesNotDeleteDirectory() throws Exception {
    Path argsDirectory = Files.createTempDirectory("odpscmd-args-dir-");
    Path nestedDirectory = Files.createDirectories(argsDirectory.resolve("nested/deep"));
    Path marker = Files.write(argsDirectory.resolve("important.sql"),
        "important".getBytes(StandardCharsets.UTF_8));
    Path nestedMarker = Files.write(nestedDirectory.resolve("deepest.sql"),
        "deepest".getBytes(StandardCharsets.UTF_8));

    try {
      try {
        CommandParserUtils.getCommandArgs(new String[]{"-I", argsDirectory.toString()});
        fail("directory args file path should be rejected");
      } catch (ODPSConsoleException e) {
        assertTrue(e.getMessage().contains("args file is not a regular file"));
      }

      assertTrue(Files.exists(argsDirectory));
      assertTrue(Files.exists(marker));
      assertTrue(Files.exists(nestedMarker));
    } finally {
      Files.deleteIfExists(nestedMarker);
      Files.deleteIfExists(nestedDirectory);
      Files.deleteIfExists(nestedDirectory.getParent());
      Files.deleteIfExists(marker);
      Files.deleteIfExists(argsDirectory);
    }
  }

}
