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

import static com.aliyun.openservices.odps.console.utils.CommandSplitter.State.COMMENT;
import static com.aliyun.openservices.odps.console.utils.CommandSplitter.State.END;
import static com.aliyun.openservices.odps.console.utils.CommandSplitter.State.ESCAPE;
import static com.aliyun.openservices.odps.console.utils.CommandSplitter.State.NORMAL;
import static com.aliyun.openservices.odps.console.utils.CommandSplitter.State.PRE_COMMENT;
import static com.aliyun.openservices.odps.console.utils.CommandSplitter.State.QUOTE;
import static com.aliyun.openservices.odps.console.utils.CommandSplitter.State.START;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

/**
 * Split a string into commands, and into tokens:
 * 1. remove comments
 * 2. remove semicolon(';')
 * 3. ensure quoted string is closed
 *
 * multi-thread unsafe!
 */
public class CommandSplitter {

  enum State {
    START, QUOTE, ESCAPE, NORMAL, PRE_COMMENT, COMMENT, END
  }

  private String input;
  private State state;
  private Character quoteType;
  private StringBuilder commandBuffer;
  private ArrayList<String> commandResults;
  private StringBuilder tokenBuffer;
  private ArrayList<String> tokenResults;
  private boolean parsed;

  public CommandSplitter(String input) {
    this.input = input;
    this.state = START;
    this.quoteType = null;
    this.commandBuffer = new StringBuilder();
    this.commandResults = new ArrayList<String>();
    this.tokenBuffer = new StringBuilder();
    this.tokenResults = new ArrayList<String>();
    this.parsed = StringUtils.isNullOrEmpty(input);
  }

  private void flushBuffer(StringBuilder buffer, ArrayList<String> results, boolean isTrim) {
    if (buffer.length() > 0) {
      String s = buffer.toString();
      String t = s.trim();
      if (t.length() > 0) {
        if (isTrim) {
          results.add(t);
        } else {
          results.add(s);
        }
      }
      buffer.setLength(0);
    }
  }

  private void flushTokenBuffer() {
    flushBuffer(tokenBuffer, tokenResults, true);
  }

  private void flushCommandBuffer() {
    flushBuffer(commandBuffer, commandResults, false);
  }

  private void normalSwitch(char c) {
    switch(c) {
      case '"':
      case '\'':
        state = QUOTE;
        quoteType = c;
        commandBuffer.append(c);
        flushTokenBuffer();
        tokenBuffer.append(c);
        break;
      case '-':
        state = PRE_COMMENT;
        break;
      case ';':
        state = NORMAL;
        flushCommandBuffer();
        flushTokenBuffer();
        break;
      case '(':
      case ')':
        state = NORMAL;
        commandBuffer.append(c);
        flushTokenBuffer();
        tokenResults.add(String.valueOf(c));
        break;
      case ' ':
      case '\t':
      case '\f':
        state = NORMAL;
        commandBuffer.append(c);
        flushTokenBuffer();
        break;
      case '\r':
      case '\n':
        state = END;
        commandBuffer.append(c);
        flushTokenBuffer();
        break;
      default:
        state = NORMAL;
        commandBuffer.append(c);
        tokenBuffer.append(c);
    }
  }

  private void parse() throws ODPSConsoleException {
    for(int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      switch(state) {
        case START:
          switch(c) {
            case ' ':
            case '\t':
            case '\f':
            case '\n':
            case '\r':
              commandBuffer.append(c);
              break;
            case '#':
              state = COMMENT;
              break;
            default:
              normalSwitch(c);
          }
          break;
        case PRE_COMMENT:
          if (c == '-') {
            state = COMMENT;
            flushTokenBuffer();
          } else {
            state = NORMAL;
            commandBuffer.append('-');
            tokenBuffer.append('-');
            i--;
          }
          break;
        case COMMENT:
          if (c == '\n' || c == '\r') {
            commandBuffer.append(c);
            state = END;
          }
          break;
        case END:
          if (c != '\n' && c != '\r') {
            state = START;
            i--;
          } else {
            commandBuffer.append(c);
          }
          break;
        case QUOTE:
          tokenBuffer.append(c);
          commandBuffer.append(c);
          if (c == quoteType) {
            state = NORMAL;
            flushTokenBuffer();
          } else if (c == '\\') {
            state = ESCAPE;
          }
          break;
        case ESCAPE:
          tokenBuffer.append(c);
          commandBuffer.append(c);
          state = QUOTE;
          break;
        case NORMAL:
          normalSwitch(c);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("impossible thing happened. pos=%s, char='%s'", i, c));
      }
    }

    switch(state) {
      case QUOTE:
      case ESCAPE:
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " string not closed");
      default:
        // in case last token/command without ";"
        flushTokenBuffer();
        flushCommandBuffer();
    }

    parsed = true;
  }

  /**
   * get split commands (trimmed and without ';')
   * return empty list if input is null or empty
   */
  public List<String> getCommands() throws ODPSConsoleException {
    if (!parsed) {
      parse();
    }
    return commandResults;
  }

  /**
   * get split tokens (trimmed and without ';')
   * return empty list if input is null or empty
   */
  public List<String> getTokens() throws ODPSConsoleException {
    if (!parsed) {
      parse();
    }
    return tokenResults;
  }

}
