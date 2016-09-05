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

package com.aliyun.openservices.odps.console.commands;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.PluginUtil;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class HelpCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"help"};

  private static final String ERR_READ_FILE = "Can not find help file.";

  public List<String> keywords;

  public void run() throws OdpsException, ODPSConsoleException {

    if (keywords == null || keywords.size() == 0) {
      InputStream is = null;
      try {
        is = this.getClass().getResourceAsStream("/readme.txt");

        if (is == null) {
          throw new ODPSConsoleException(ERR_READ_FILE);
        }

        getWriter().writeResult(IOUtils.readStreamAsString(is));
        getWriter().writeResult("\nUse 'help [keywords...] to search for more detail.");
      } catch (Exception e) {

        throw new ODPSConsoleException(ERR_READ_FILE);
      } finally {
        if (is != null) {
          try {
            is.close();
          } catch (IOException e) {
          }
        }
      }
    } else if (keywords.size() == 1 && keywords.get(0).equalsIgnoreCase("command")) {
      PluginUtil.printPluginCommandPriority();
    } else {
      // compare the keys and tags of each command
      CommandParserUtils.printHelpInfo(keywords);
    }
  }

  public HelpCommand(String commandText, List<String> keywords, ExecutionContext context) {
    super(commandText, context);
    this.keywords = keywords;
  }

  /**
   * 通过传递的参数，解析出对应的command
   **/
  public static HelpCommand parse(List<String> optionList, ExecutionContext sessionContext) {
    if (optionList.contains("-h") && optionList.size() == 1) {

      // 消费掉"-h"
      optionList.remove(optionList.indexOf("-h"));
      return new HelpCommand("-h", null, sessionContext);

    } else if (optionList.contains("--help") && optionList.size() == 1) {
      // 消费掉"--help"
      optionList.remove(optionList.indexOf("--help"));
      return new HelpCommand("--help", null, sessionContext);
    }

    return null;
  }

  public static HelpCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    if (commandString.toUpperCase().matches("\\s*H(ELP)?(\\s*|\\s+.+)")) {
      AntlrObject antlr = new AntlrObject(commandString.toLowerCase());
      String[] parts = antlr.getTokenStringArray();
      List<String> keywords = new ArrayList<String>(Arrays.asList(parts));
      keywords.remove(0);   // remove the help itself
      return new HelpCommand(commandString, keywords, sessionContext);
    }
    return null;
  }

  public static void printUsage(PrintStream out) {
    out.println("");
    out.println("Usage: help");
    out.println("       help [keyword1 keyword2 ...]");
    out.println("For example:");
    out.println("       help");
    out.println("       help create table");
  }
}
