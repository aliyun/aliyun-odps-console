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

import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Function;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

/**
 * Created by nizheming on 15/8/24.
 */
public class DescribeFunctionCommand extends AbstractCommand {

  private final String functionName;

  public DescribeFunctionCommand(String functionName, String cmd, ExecutionContext context) {
    super(cmd, context);
    this.functionName = functionName;
  }

  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+FUNCTION(\\s+(.*))",
                                                   Pattern.CASE_INSENSITIVE);
  public static AbstractCommand parse(String cmd, ExecutionContext ctx) throws ODPSConsoleException {
    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    if (m.groupCount() < 3) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + " Must Specific Function Name.");
    }

    String name = m.group(3);
    return new DescribeFunctionCommand(name, cmd, ctx);
  }

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    Function function = getCurrentOdps().functions().get(functionName);
    function.reload();
    System.out.println(String.format("%-40s%-40s", "Name", functionName));
    System.out.println(String.format("%-40s%-40s", "Owner", function.getOwner()));
    System.out.println(String.format("%-40s%-40s", "Created Time", DATE_FORMAT.format(function.getCreatedTime())));
    System.out.println(String.format("%-40s%-40s", "Class", function.getClassPath()));
    StringBuilder builder = new StringBuilder();
    for (Resource r : function.getResources()) {
      if (builder.length() != 0) {
        builder.append(",");
      }
      builder.append(r.getName());
    }

    System.out.println(String.format("%-40s%-40s", "Resources", builder.toString()));


  }
}
