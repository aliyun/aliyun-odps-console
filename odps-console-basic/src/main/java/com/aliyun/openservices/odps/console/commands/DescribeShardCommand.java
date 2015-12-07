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

/**
 * Created by yinyue on 15-3-18.
 */

import com.aliyun.odps.*;
import com.aliyun.openservices.odps.console.ErrorCode;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Describe shard meta
 *
 * DESCRIBE|DESC SHARD table_name
 *
 * @author <a
 *         href="yingyue.yy@alibaba-inc.com">yinyue.yy@alibaba-inc.com
 *         </a>
 *
 */
public class DescribeShardCommand extends AbstractCommand {

  private String project;
  private String table;

  DescribeShardCommand(String cmd, ExecutionContext cxt, String project, String table) {
    super(cmd, cxt);
    this.project = project;
    this.table = table;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    if (table == null || table.length() == 0) {
      throw new OdpsException(
              ErrorCode.INVALID_COMMAND
                      + ": Invalid syntax - DESCRIBE|DESC SHARD table_name;");
    }

    DefaultOutputWriter writer = this.getContext().getOutputWriter();

    Odps odps = getCurrentOdps();

    if (project == null) {
      project = getCurrentProject();
    }

    Table t = odps.tables().get(project, table);

    writer.writeResult(""); // for HiveUT

    Shard shard = t.getShard();
    String result = getScreenDisplay(shard);

    writer.writeResult(result);
    System.out.flush();

    writer.writeError("OK");
  }

  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+(SHARD)\\s+(.*)", Pattern.CASE_INSENSITIVE);

  public static DescribeShardCommand parse(String cmd, ExecutionContext cxt) {

    if (cmd == null || cxt == null) {
      return null;
    }

    DescribeShardCommand r = null;
    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();
    if (!match || m.groupCount() != 3) {
      return r;
    }

    cmd = m.group(3);
    ODPSConsoleUtils.TablePart tablePart = ODPSConsoleUtils.getTablePart(cmd);
    if (tablePart.tableName != null) {
      String[] tableSpec = ODPSConsoleUtils.parseTableSpec(tablePart.tableName);
      String project = tableSpec[0];
      String table = tableSpec[1];

      r = new DescribeShardCommand(cmd, cxt, project, table);
    }
    return r;
  }

  private String getScreenDisplay(Shard shard) throws ODPSConsoleException {
    StringWriter out = new StringWriter();
    PrintWriter w = new PrintWriter(out);

    try {
      if (shard != null) {
        w.println("+------------------------------------------------------------------------------------+");
        if (shard.getHubLifecycle() == -1) {
          w.printf("| HubLifecycle:             %-56s |\n", "Not available");
        } else {
          w.printf("| HubLifecycle:             %-56s |\n", shard.getHubLifecycle());
        }

        w.println("+------------------------------------------------------------------------------------+");
        w.printf("| ShardNum:                 %-56s |\n", shard.getShardNum());
        w.println("+------------------------------------------------------------------------------------+");

      } else {
        throw new ODPSConsoleException(ErrorCode.INVALID_COMMAND + "Shard does not exist");
      }
    } catch (Exception e) {
      getContext().getOutputWriter().writeDebug("Invalid shard meta");
      throw new ODPSConsoleException(ErrorCode.INVALID_RESPONSE + ": Invalid shard schema.", e);
    }

    w.flush();
    w.close();

    return out.toString();
  }
}
