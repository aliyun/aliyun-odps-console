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

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Describe table meta
 * 
 * DESCRIBE|DESC table_name [PARTITION(partition_col = 'partition_col_value',
 * ...)];
 * 
 * @author <a
 *         href="shenggong.wang@alibaba-inc.com">shenggong.wang@alibaba-inc.com
 *         </a>
 * 
 */
public abstract class DescribeCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"describe", "desc"};

  // Each subtype of desc command is not visible in CommandParserUtils, so group all help info here together
  public static void printUsage(PrintStream stream) {
    stream.println("Usage: describe|desc [extended] [<projectname>.]<tablename> [partition(<spec>)]");
    stream.println("       describe|desc instance <instanceID>");
    stream.println("       describe|desc project [-extended] <projectname>");
    stream.println("       describe|desc function <functionname>");
    stream.println("       describe|desc resource [-p,-project <projectname>] <resourcename>");
    stream.println("       describe|desc resource [<projectname>.]<resourcename>");
    stream.println("       describe|desc shard [<projectname>.]<tablename> [partition (<spec>)]");
  }

  private DescribeCommand(String cmd, ExecutionContext ctx) {
    super(cmd, ctx);
  }

  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+(.*)",
      Pattern.CASE_INSENSITIVE|Pattern.DOTALL);


  public static AbstractCommand parse(String cmd, ExecutionContext ctx)
      throws ODPSConsoleException {
    if (cmd == null || ctx == null) {
      return null;
    }

    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return null;
    }

    AbstractCommand command = null;
    
    command = DescribeResourceCommand.parse(cmd, ctx);
    if (command != null) {
      return command;
    }

    command = DescribeProjectCommand.parse(cmd, ctx);
    if (command != null) {
      return command;
    }

    command = DescribeFunctionCommand.parse(cmd, ctx);
    if (command != null) {
      return command;
    }
    
    command = DescribeTableExtendedCommand.parse(cmd, ctx);
    if (command != null) {
      return command;
    }

    command = DescribeShardCommand.parse(cmd, ctx);
    if (command != null) {
      return command;
    }

    command = DescribeInstanceCommand.parse(cmd, ctx);
    if (command != null) {
      return command;
    }

    command = DescribeTableCommand.parse(cmd, ctx);
    if (command != null) {
      return command;
    }

    return null;
  }



}
