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
import org.junit.Test;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

/**
 * This UT have two parts:
 *
 *    Part 1. Parse cmds and check if generated cmd array are correct
 *            empty commands and space commands are ignore in result
 *            space and newlines are reserved
 *    Part 2. Parse one cmd and get tokens (w/o paren merged)
 *
 *    Note: semicolon/comments will be removed.
 */
public class AntlrObjectTest {

  // Part 1.
  @Test
  public void testParseOneCommand() throws ODPSConsoleException {
    // Before going into AntlrObject, the command SHOULD have ';' at end yet
    // But this is not required for AntlrObject

    // Simple cases:

    checkParseOneCommandResult("",
                               new String[]{});

    checkParseOneCommandResult("select * from t1;",
                               new String[]{"select", "*", "from", "t1"}
    );

    // Comments:
    // Test:
    //   # must start at begin of line, or after some spaces or tabs
    //   -- can be at any position of line
    // Node:
    //   all of \n and spaces will be reserved
    checkParseOneCommandResult("\t code;--comment",
                               new String[]{"code"}
    );

    checkParseOneCommandResult(" \t#comment",
                               new String[]{}
    );

    checkParseOneCommandResult("code#code",
                               new String[]{"code#code"}
    );

    checkParseOneCommandResult("code--comment",
                               new String[]{"code"}
    );

    checkParseOneCommandResult("code-code",
                               new String[]{"code-code"}
    );

    // Parens
    checkParseOneCommandResult(
        "#comments\nadd #table proj1.table1 partition(p1=\"1\",p2=\"1\") as   resource1",
        new String[]{"add", "#table", "proj1.table1", "partition", "(", "p1=", "\"1\"", ",p2=",
                     "\"1\"", ")", "as", "resource1"});
  }
  // Part 2.
  @Test
  public void testParseCommands() throws ODPSConsoleException {
    // Empty
    checkParseCommandsResult(";  ;  ;  ;",
                             new String[]{});

    // Simple case
    checkParseCommandsResult("ls resources;\nselect * from t1;",
                             new String[]{"ls resources", "\nselect * from t1"});

    checkParseCommandsResult("### comment\n" +
                             "### comment\n" +
                             "code\n" +
                             "code--comments\n" +
                             "### comment\n" +
                             ";",
                             new String[]{"\n\ncode\ncode\n\n"}
    );
  }

  private void checkParseOneCommandResult(String cmd, String[] words)
      throws ODPSConsoleException {
    System.out.print("UT cmd:[" + cmd + "]");
    AntlrObject obj = new AntlrObject(cmd);
    assertArrayEquals(words, obj.getTokenStringArray());
    System.out.println("............OK");
  }

  private void checkParseCommandsResult(String cmds, String[] cmdArray)
      throws ODPSConsoleException {
    System.out.print("UT cmds:[" + cmds + "]");
    AntlrObject obj = new AntlrObject(cmds);
    assertArrayEquals(cmdArray, obj.splitCommands().toArray());
    System.out.println("............OK");
  }
}
