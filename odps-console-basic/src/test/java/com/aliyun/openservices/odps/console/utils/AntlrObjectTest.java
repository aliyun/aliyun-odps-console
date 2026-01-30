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

import java.util.Arrays;
import java.util.stream.Stream;

import com.aliyun.openservices.odps.console.ExecutionContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  // ===================================================================================================================
  // 1. test tokenize
  static Stream<Arguments> tokenizeSimpleStatement() {
    return Stream.of(
            Arguments.of("1", new String[]{"1"}),
            Arguments.of("select * from t1;", new String[]{"select", "*", "from", "t1"}),
            Arguments.of("drop resource a.jar", new String[]{"drop", "resource", "a.jar"}));
  }

  // Comments:
  // Test:
  //   # must start at begin of line, or after some spaces or tabs
  //   -- can be at any position of line
  // Node:
  //   all of \n and spaces will be reserved
  static Stream<Arguments> tokenizeComment() {
    return Stream.of(
            Arguments.of("\t code;--comment", new String[]{"code"}),
            Arguments.of(" \t#comment", new String[]{}),
            Arguments.of("code#code", new String[]{"code#code"}),
            Arguments.of("code--comment", new String[]{"code"}),
            Arguments.of("code-code", new String[]{"code-code"})
    );
  }

  static Stream<Arguments> tokenizeComplex() {
    return Stream.of(
            Arguments.of("#comments\nadd #table proj1.table1 partition(p1=\"1\",p2=\"1\") as   resource1",
                    new String[]{"add", "#table", "proj1.table1", "partition", "(", "p1=", "\"1\"", ",p2=",
                            "\"1\"", ")", "as", "resource1"}),
            Arguments.of("hello--world\n\r\r\n\r\r\n  \n\f a-b-c\n",
                    new String[]{"hello", "a-b-c"}),
            Arguments.of(" '\"' \"'\" \"\\\"\" '\\'' ",
                    new String[]{"'\"'", "\"'\"", "\"\\\"\"", "'\\''"}),
            Arguments.of("  # abc\nhello--world\nselect a-b -c, \"\\\"\", '\"', " +
                            "\"'\", (\\\\\\a\\b\\c), \"abc\ndef\", \"xy\n\r\r\nz\" " +
                            "\r\r\n \n\r  \nlimit 1 #c",
                    new String[]{"hello", "select", "a-b", "-c,", "\"\\\"\"", ",",
                            "'\"'", ",", "\"'\"", ",",
                            "(", "\\\\\\a\\b\\c", ")", ",",
                            "\"abc\ndef\"", ",", "\"xy\n\r\r\nz\"",
                            "limit", "1", "#c"})
    );
  }

  static Stream<Arguments> tokenizeRawString() throws ODPSConsoleException {
    return Stream.of(
            Arguments.of("select R\"###(this is a test'\";\nafter quotations)###\"", new String[]{
                    "select", "R\"###(", "this is a test'\";\nafter quotations", ")###\""
            }),
            Arguments.of("select R'###(this is a test' of not close;", new String[] {
                    "select", "R", "'###(this is a test'", "of", "not", "close"
            }),
            Arguments.of("R'abc'", new String[] {"R", "'abc'"}),
            Arguments.of("CALL EXEC_EXTERNAL_QUERY('hologres_fs'," +
                    "r\"###(call hg_insert_overwrite('parent_table','20230421'," +
                    "$$select * from test1 where partition_value='20230421'$$);)###\");", new String[] {
                            "CALL", "EXEC_EXTERNAL_QUERY", "(", "'hologres_fs'",  ",",
                                    "r\"###(", "call hg_insert_overwrite('parent_table','20230421'," +
                                    "$$select * from test1 where partition_value='20230421'$$);", ")###\"", ")"
            })
    );
  }

  @ParameterizedTest
  @MethodSource({
          "tokenizeSimpleStatement",
          "tokenizeComment",
          "tokenizeComplex",
          "tokenizeRawString"
  })
  void testTokenizer(String input, String[] words) throws ODPSConsoleException {
    System.out.print("input: " + input);

    AntlrObject obj = new AntlrObject(input);
    Assertions.assertArrayEquals(words, obj.getRawTokenStringArray());
    System.out.println("tokenize result: " + Arrays.toString(obj.getTokenStringArray()));
  }

  // ===================================================================================================================
  // 2. test split statement

  static Stream<Arguments> splitBasicInput() {
    return Stream.of(
            Arguments.of("; ; ; ;", new String[]{}),  // empty
            Arguments.of("ls resources;\nselect * from t1;", new String[]{
                    "ls resources", "\nselect * from t1"
            }),
            Arguments.of("### comment\n" +
                    "### comment\n" +
                    "code\n" +
                    "code--comments\n" +
                    "### comment\n" +
                    ";",
                    new String[]{"\n\ncode\ncode\n\n"}),
            Arguments.of("code1;--comment\ncode2--comment\ncode3", new String[]{
                    "code1", "\ncode2\ncode3"
            })
    );
  }

  static Stream<Arguments> splitRawString() {
    return Stream.of(
            Arguments.of("select R\"###(this is a test'\";after quotations)###\"", new String[]{
                    "select R\"###(this is a test'\";after quotations)###\""
            })
    );
  }

  @ParameterizedTest
  @MethodSource({
          "splitBasicInput",
          "splitRawString"
  })
  void testCommandSplit(String input, String[] cmdArray) throws ODPSConsoleException {

    System.out.print("input: " + input);
    AntlrObject obj = new AntlrObject(input);
    Assertions.assertArrayEquals(cmdArray, obj.splitCommands(ExecutionContext.init()).toArray());
    System.out.println("split result: " + Arrays.toString(obj.getTokenStringArray()));
    System.out.println();
  }
}
