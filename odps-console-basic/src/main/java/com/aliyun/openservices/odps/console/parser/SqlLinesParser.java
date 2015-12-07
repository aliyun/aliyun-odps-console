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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

@Deprecated
public class SqlLinesParser {
  private enum Status {
    LINESTART_MODE, STRING_MODE, QUERY_MODE, COMMENT_MODE
  }

  public static Map<String, List<String>> lineParser(String input) {
    boolean continueFlag = false;
    Status mStatus = Status.LINESTART_MODE;
    StringBuilder mCurCmd = new StringBuilder("");
    StringBuilder mCurPrintcmd = new StringBuilder("");
    char stringmodeKeyWord = '-';
    char mDelimiter = ';';
    ArrayList<String> cmds = new ArrayList<String>();
    ArrayList<String> printcmds = new ArrayList<String>();

    for (int i = 0; i < input.length();) {
      switch (mStatus) {
      case LINESTART_MODE:
        switch (input.charAt(i)) {
        case ' ':
        case '\t':
        case '\n':
          if (continueFlag) {
            mCurCmd.append(input.charAt(i));
          }
          mCurPrintcmd.append(input.charAt(i));
          ++i;
          continue;
        case '-':
          if (((i + 1) < input.length()) && ('-' == input.charAt(i + 1))) {
            mCurPrintcmd.append(input.charAt(i));
            ++i;
          } else {
            mStatus = Status.QUERY_MODE;
            stringmodeKeyWord = '-';
            continue;
          }
        case '#':
          mCurPrintcmd.append(input.charAt(i));
          mStatus = Status.COMMENT_MODE;
          ++i;
          continue;
        default:
          mStatus = Status.QUERY_MODE;
          stringmodeKeyWord = '-';
          continue;
        }

      case STRING_MODE:
        if ('\\' == input.charAt(i)) {
          mCurCmd.append(input.charAt(i));
          mCurPrintcmd.append(input.charAt(i));
          ++i;
          if (i < input.length()) {
            mCurCmd.append(input.charAt(i));
            mCurPrintcmd.append(input.charAt(i));
            ++i;
          }
        } else if (mDelimiter == input.charAt(i)) {

          // mCurCmd.append('\\');
          mCurCmd.append(input.charAt(i));
          mCurPrintcmd.append(input.charAt(i));
          ++i;
        } else {
          if (stringmodeKeyWord == input.charAt(i)) {
            mStatus = Status.QUERY_MODE;
            stringmodeKeyWord = '-';
          }
          mCurCmd.append(input.charAt(i));
          mCurPrintcmd.append(input.charAt(i));
          ++i;
        }
        continue;
      case QUERY_MODE:
        if (mDelimiter == input.charAt(i)) {
          mCurPrintcmd.append(input.charAt(i));
          cmds.add(mCurCmd.toString());
          printcmds.add(mCurPrintcmd.toString());
          mCurCmd = new StringBuilder("");
          mCurPrintcmd = new StringBuilder("");
          continueFlag = false;
          ++i;
        } else if (('-' == input.charAt(i)) && ((i + 1) < input.length())
            && ('-' == input.charAt(i + 1))) {
          mStatus = Status.COMMENT_MODE;
          mCurPrintcmd.append("--");
          i = i + 2;
        } else if (('\n' == input.charAt(i)) || (' ' == input.charAt(i))
            || ('\t' == input.charAt(i))) {
          if ('\n' == input.charAt(i)) {
            mStatus = Status.LINESTART_MODE;
          }
          if (continueFlag) {
            mCurCmd.append(input.charAt(i));
          }
          mCurPrintcmd.append(input.charAt(i));
          ++i;
        } else {
          if (('\'' == input.charAt(i)) || ('"' == input.charAt(i)) || ('`' == input.charAt(i))) {
            mStatus = Status.STRING_MODE;
            stringmodeKeyWord = input.charAt(i);
          }
          continueFlag = true;
          mCurCmd.append(input.charAt(i));
          mCurPrintcmd.append(input.charAt(i));
          ++i;
        }
        continue;
      case COMMENT_MODE:
        if ('\n' == input.charAt(i)) {
          mStatus = Status.LINESTART_MODE;
        } else {
          mCurPrintcmd.append(input.charAt(i));
          ++i;
        }
        continue;
      }
    }

    if (mCurCmd.length() != 0) {
      cmds.add(mCurCmd.toString());
    }
    
    if (mCurPrintcmd.length() != 0) {
      printcmds.add(mCurPrintcmd.toString());
    }
    
    Map<String, List<String>> map = new HashMap<String, List<String>>();
    map.put(ODPSConsoleConstants.SQLCOMMANDS, cmds);
    map.put(ODPSConsoleConstants.PRINTCMDS, printcmds);
    return map;
  }

}
