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

package com.aliyun.openservices.odps.console.auth;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;

public class SetProjectCommand extends AbstractCommand {

  private String commandText;

  public static final String[] HELP_TAGS = new String[]{"setproject", "set", "project"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: setproject <key>=<value> [<key>=<value>]");
  }

  public SetProjectCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
    this.commandText = commandText;

  }

  @Override
  public void run() throws ODPSConsoleException {
    try {
      Odps odps = getCurrentOdps();
      Project project = odps.projects().get();

      if (commandText.isEmpty()) {
        Map<String, String> allProperties = project.getAllProperties();

        if (allProperties != null) {
          // print all properties
          for (Map.Entry<String, String> property : allProperties.entrySet()) {
            getWriter().writeError(property.getKey() + "=" + property.getValue());
          }
        }
        getWriter().writeError("OK");
        return;
      }
      Map<String, String> properties = parseProperties();
      odps.projects().updateProject(getCurrentProject(), properties);

      getWriter().writeError("OK");
    } catch (OdpsException e) {
      throw new ODPSConsoleException(e.getMessage(), e);
    }
  }

  public static SetProjectCommand parse(String commandString, ExecutionContext context) {

    if (commandString.toUpperCase().matches("^SETPROJECT\\s*[\\s\\S]*")) {
      return new SetProjectCommand(commandString.substring(10).trim(), context);
    }
    return null;
  }

  /**
   * 计算未闭合的大括号、小括号的数量。只有brackets为0时，认为是一个完整的property
   */
  private int brackets = 0;
  /**
   * 计算双引号、单引号的数量。只有quotation % 2 == 0（引号闭合）时，认为是一个完整的property
   */
  private int quotations = 0;

  /**
   * 将字符串parse成properties map
   * 多个properties采用空格分割，key和value之间用等号连接
   * 其中value在双引号、大小括号内的空格会认为是value的一部分
   */
  public Map<String, String> parseProperties() {
    Map<String, String> properties = new HashMap<>(0);

    String input = commandText + " ";
    StringBuilder key = new StringBuilder();
    StringBuilder value = new StringBuilder();
    boolean keyMode = true;

    for (int index = 0; index < input.length(); index++) {
      char c = input.charAt(index);
      if (keyMode) {
        if (c == '=') {
          keyMode = false;
          continue;
        } else if (isEndOfProperty(c)) {
          save(properties, key, null);
          continue;
        }
        key.append(c);
      } else {
        if (isEndOfProperty(c) && isCompleteProperty()) {
          save(properties, key, value);
          keyMode = true;
          continue;
        }
        value.append(c);
        updateQuotationsAndBracketsCount(c);
      }
    }
    return properties;
  }

  private boolean isEndOfProperty(char c) {
    // equals \s+
    return c == '\f' || c == '\n' || c == '\r' || c == '\t' || c == ' ';
  }

  private boolean isCompleteProperty() {
    return brackets == 0 && (quotations % 2 == 0);
  }

  private void save(Map<String, String> properties, StringBuilder key, StringBuilder value) {
    if (key.length() == 0) {
      return;
    }
    if (value == null) {
      properties.put(key.toString(), "");
      key.setLength(0);
    } else {
      properties.put(key.toString(), value.toString());
      key.setLength(0);
      value.setLength(0);
    }
  }

  private void updateQuotationsAndBracketsCount(char c) {
    if (c == '{' || c == '(') {
      brackets++;
    } else if (c == '}' || c == ')') {
      brackets--;
    } else if (c == '\'' || c == '\"') {
      quotations++;
    }
  }
}
