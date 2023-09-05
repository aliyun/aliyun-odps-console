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

import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class UseSchemaCommand extends DirectCommand {

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
  }

  public static final String[] HELP_TAGS = new String[]{"use", "schema"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: use schema <schema name>;");
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*USE\\s+SCHEMA\\s+(\\w+)\\s*", Pattern.CASE_INSENSITIVE);

  private final String schemaName;

  public UseSchemaCommand(
      String commandText,
      ExecutionContext context,
      String schemaName) {

    super(commandText, context);
    this.schemaName = schemaName;
  }

  public static SetCommand parse(String commandString, ExecutionContext ctx)
      throws ODPSConsoleException {
    Matcher matcher = PATTERN.matcher(commandString);
    if (matcher.matches()) {
      String schemaName = matcher.group(1);


      return new SetCommand(true, SetCommand.SQL_DEFAULT_SCHEMA, schemaName, commandString, ctx);
    }
    return null;
  }
}
