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

package com.aliyun.openservices.odps.console.utils.antlr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.ParseTree;

import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;


/**
 * This class is used in two steps in console:
 * 1. splitCommands cmds into separate commands
 * plz call splitCommands()
 * 2. parse one command and return tokens
 * plz call getTokenStringArray() or getTokenStringArrayWithParenMerged()
 *
 * because antlr's parser can only scan once,
 * so user should just choose one of the two functions above.
 */
public class AntlrObject {

  private OdpsCmdLexer lexer;
  private OdpsCmdParser parser;

  private CommonTokenStream tokenStream;
  private VerboseListener listener;

  public String originalCommand;

  public AntlrObject(String rawCommand) throws ODPSConsoleException {
    originalCommand = rawCommand;
  }

  private void prepareParseTree() throws ODPSConsoleException {
    ANTLRInputStream input = new ANTLRInputStream(originalCommand);
    lexer = new OdpsCmdLexer(input);

    lexer.removeErrorListeners();
    listener = new VerboseListener();
    lexer.addErrorListener(listener);

    tokenStream = new CommonTokenStream(lexer);
    tokenStream.fill();
    tokenStream.getTokens();
    parser = new OdpsCmdParser(tokenStream);

    if (listener.errorMsg != null && !listener.errorMsg.equals("")) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " " + listener.errorMsg);
    }
  }

  /**
   * Parse one command
   *
   * @throws ODPSConsoleException
   */
  public String[] getTokenStringArray() throws ODPSConsoleException {

    prepareParseTree();
    List<String> cmdParts = new ArrayList<String>();

    ParseTree cmd = parser.cmds().children.get(0);
    if (cmd.getChildCount() != 0) {
      for (ParseTree cmdPart : ((OdpsCmdParser.CmdContext) cmd).children) {
        int ruleIndex = ((ParserRuleContext) cmdPart).getRuleIndex();
        if (ruleIndex != OdpsCmdParser.RULE_space &&
            ruleIndex != OdpsCmdParser.RULE_newline &&
            ruleIndex != OdpsCmdParser.RULE_comment &&
            ruleIndex != OdpsCmdParser.RULE_semicolon) {

          String text = cmdPart.getText();

          cmdParts.add(text);
        }
      }
    }

    return cmdParts.toArray(new String[]{});
  }

  /**
   * Split commands into command list:
   * 1. remove comments
   * 2. split by semicolon except those in STRING tokens
   * 3. reconstruct each command to it's original form
   */
  public List<String> splitCommands() throws ODPSConsoleException {
    prepareParseTree();
    String oneCommand = "";
    List<String> commandList = new ArrayList<String>();

    for (ParseTree cmd : parser.cmds().children) {
      // when it is semicolon
      if (((ParserRuleContext) cmd).getRuleIndex() != OdpsCmdParser.RULE_cmd ||
          cmd.getText().equals("")) {
        continue;
      }

      for (ParseTree cmdPart : ((OdpsCmdParser.CmdContext) cmd).children) {
        if (((ParserRuleContext) cmdPart).getRuleIndex() == OdpsCmdParser.RULE_comment) {
          String tmp = cmdPart.getText();
          if (!tmp.startsWith("--")) {
            oneCommand += tmp.substring(0, tmp.indexOf('#'));
          }
        } else {
          oneCommand += cmdPart.getText();
        }
      }
      if (!oneCommand.trim().equals("")) {
        commandList.add(oneCommand);
      }
      oneCommand = "";
    }

    return commandList;
  }
}
