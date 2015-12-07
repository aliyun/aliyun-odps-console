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

package com.aliyun.openservices.odps.console.parser;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

/**
 * Created by nizheming on 15/7/21.
 */
public class SqlLinesParserTest {

  @Test
  public void testLinesParser() {
    Map<String, List<String>> map = SqlLinesParser.lineParser("select * from src;set 'a=b;");
    List<String> commands = map.get(ODPSConsoleConstants.SQLCOMMANDS);
    assertEquals(commands.size(),2);
    assertEquals(commands.get(0), "select * from src");
    assertEquals(commands.get(1), "set 'a=b;");
  }

  @Test
  public void testLinesParser1() {
    Map<String, List<String>> map = SqlLinesParser.lineParser("select * from src;set 'a=b';");
    List<String> commands = map.get(ODPSConsoleConstants.SQLCOMMANDS);
    assertEquals(commands.size(),2);
    assertEquals(commands.get(0), "select * from src");
    assertEquals(commands.get(1), "set 'a=b'");
  }

  @Test
  public void testLinesParser2() {
    Map<String, List<String>> map = SqlLinesParser.lineParser("set 'a=b;");
    List<String> commands = map.get(ODPSConsoleConstants.SQLCOMMANDS);
    assertEquals(commands.size(),1);
    assertEquals(commands.get(0), "set 'a=b;");
  }

}