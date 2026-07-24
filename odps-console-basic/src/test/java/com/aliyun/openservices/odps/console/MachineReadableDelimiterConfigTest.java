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

package com.aliyun.openservices.odps.console;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Assert;
import org.junit.Test;

public class MachineReadableDelimiterConfigTest {

  @Test
  public void testDefaultDelimiter() {
    Assert.assertEquals(',', new ExecutionContext().getMachineReadableDelimiter());
  }

  @Test
  public void testCustomDelimiter() throws Exception {
    ExecutionContext context = loadContext("machine_readable_delimiter=|\n");

    Assert.assertEquals('|', context.getMachineReadableDelimiter());
  }

  @Test
  public void testTabDelimiter() throws Exception {
    ExecutionContext context = loadContext("machine_readable_delimiter=\\t\n");

    Assert.assertEquals('\t', context.getMachineReadableDelimiter());
  }

  @Test
  public void testSpaceDelimiter() throws Exception {
    ExecutionContext context = loadContext("machine_readable_delimiter=\\ \n");

    Assert.assertEquals(' ', context.getMachineReadableDelimiter());
  }

  @Test
  public void testRejectMultiCharacterDelimiter() throws Exception {
    try {
      loadContext("machine_readable_delimiter=||\n");
      Assert.fail("Expected invalid delimiter configuration to fail");
    } catch (ODPSConsoleException e) {
      Assert.assertTrue(e.getCause().getMessage().contains("must be a single character"));
    }
  }

  private ExecutionContext loadContext(String config) throws Exception {
    Path configFile = Files.createTempFile("odps-config-", ".ini");
    try {
      Files.write(configFile, config.getBytes(StandardCharsets.UTF_8));
      return ExecutionContext.load(configFile.toString());
    } finally {
      Files.deleteIfExists(configFile);
    }
  }
}
