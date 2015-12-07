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

import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;
import org.antlr.v4.runtime.Token;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class AntlrObjectTestExt {

  @Test
  public void testSplitCommands2() throws ODPSConsoleException {
    String test = "--\n" +
            "##\n" +
            " --\n" +
            " #--\n" +
            "\t --\n" +
            " \t #-\n" +
            "select *  --ab#$%^&\n" +
            "\n" +
            "   #--\n" +
            "\n" +
            "from --ab#$%^&\n" +
            "  dual01; \n" +
            "\n" +
            "#-;";
    List<String> commandList = new AntlrObject(test).splitCommands();
    assertEquals(1, commandList.size());
  }

  @Test
  public void testSplitCommands3() throws ODPSConsoleException {
    String test3 = "--a'\"'\"\"\"'b#$%^&;\n" +
            "##a'\"'\"\"\"'b#$%^&;\n" +
            " --a'\"'\"\"\"'b#$%^&;\n" +
            " #--a'\"'\"\"\"'b#$%^&;\n" +
            "\t #a'\"'\"\"\"'b#$%^&;\n" +
            " \t #-\n" +
            "select *  #a'\"'\"\"\"'b#$%^&;\n" +
            "\n" +
            "   #--a'\"'\"\"\"'b#$%^&;\n" +
            "\n" +
            "from #a'\"'\"\"\"'b#$%^&\n" +
            "  dual01; \n" +
            "\n" +
            "#-;a'\"'\"\"\"'b#$%^&;";
    List<String> commandList = new AntlrObject(test3).splitCommands();
    assertEquals(2, commandList.size());
  }
}
