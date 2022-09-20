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

package com.aliyun.openservices.odps.console.pub;

import static com.aliyun.openservices.odps.console.utils.Coordinate.getCoordinateABC;
import static com.aliyun.openservices.odps.console.utils.Coordinate.getCoordinateOptionP;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.CommandWithOptionP;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * List partitions
 * <p/>
 * SHOW PARTITIONS [project_name.]<table_name>;
 *
 * @author <a
 * href="shenggong.wang@alibaba-inc.com">shenggong.wang@alibaba-inc.com
 * </a>
 */
public class ShowPartitionsCommand extends AbstractCommand {

  public static final String[]
      HELP_TAGS =
      new String[]{"show", "list", "ls", "partition", "partitions"};

  public static void printUsage(PrintStream stream, ExecutionContext ctx) {
    // legacy usage: list|ls partitions [-p,-project <project name>] <table name> [(<spec>)]
    if (ctx.isProjectMode()) {
      stream.println("Usage: show partitions [<project name>.]<table name>"
                     + " [partition(<spec>)]");
    } else {
      stream.println("Usage: show partitions [[<project name>.]<schema name>.]<table name>"
                     + " [partition(<spec>)]");
    }
  }

  private Coordinate coordinate;

  public ShowPartitionsCommand(String cmd, ExecutionContext cxt, Coordinate coordinate) {
    super(cmd, cxt);
    this.coordinate = coordinate;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String project = coordinate.getProjectName();
    String schema = coordinate.getSchemaName();
    String table = coordinate.getObjectName();
    String partition = coordinate.getPartitionSpec();

    Odps odps = OdpsConnectionFactory.createOdps(getContext());
    Table t = odps.tables().get(project, schema, table);
    Iterator<Partition> parts;
    if (partition != null) {
      parts = t.getPartitionIterator(new PartitionSpec(partition));
    } else {
      parts = t.getPartitionIterator();
    }

    DefaultOutputWriter writer = getContext().getOutputWriter();
    writer.writeResult(""); // for HiveUT
    while (parts.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();

      String p = parts.next().getPartitionSpec().toString();
      p = p.replaceAll("\'", ""); // 兼容旧版本不带引号的输出格式
      p = p.replaceAll(",", "/"); // 兼容SQLTask输出的格式-。-
      writer.writeResult(p);
    }
    writer.writeError("\nOK");
  }

  private static final Pattern PREFIX = Pattern.compile(
      "\\s*(SHOW|LS|LIST)\\s+PARTITIONS\\s+.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern PATTERN = Pattern.compile(
      "\\s*SHOW\\s+PARTITIONS\\s+" + Coordinate.TABLE_PATTERN,
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern PUBLIC_PATTERN =
      Pattern.compile("\\s*(LS|LIST)\\s+PARTITIONS\\s+" + Coordinate.PUB_TABLE_PATTERN,
                      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern PUBLIC_PREFIX_PATTERN =
      Pattern.compile("\\s*(LS|LIST)\\s+PARTITIONS\\s+(.*)",
                      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static ShowPartitionsCommand parse(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {
    // 1. check match
    if (!PREFIX.matcher(cmd).matches()) {
      return null;
    }

    // 1. match list partitions
    Coordinate coordinate = parseListPartitionsCommand(cmd, cxt);
    if (coordinate == null) {
      // 2. match show partitions
      Matcher showMatcher = PATTERN.matcher(cmd);
      if (!showMatcher.matches()) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }
      coordinate = Coordinate.getTableCoordinate(showMatcher, cxt);
    }

    return new ShowPartitionsCommand(cmd, cxt, coordinate);
  }

  public static Coordinate parseListPartitionsCommand(String cmdTxt, ExecutionContext ctx)
      throws ODPSConsoleException {
    Matcher preMatcher = PUBLIC_PREFIX_PATTERN.matcher(cmdTxt);
    if (!preMatcher.matches()) {
      return null;
    }

    CommandWithOptionP cmd = new CommandWithOptionP(cmdTxt);
    cmdTxt = cmd.getCmd();

    Matcher lsMatcher = PUBLIC_PATTERN.matcher(cmdTxt);
    if (!lsMatcher.matches()) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }

    String table = lsMatcher.group(Coordinate.TABLE_GROUP);
    String partitionSpec = lsMatcher.group(Coordinate.PARTITION_GROUP);

    Coordinate coordinate = getCoordinateABC(table);
    if (cmd.hasOptionP() && !table.contains(".")) {
      coordinate = getCoordinateOptionP(cmd.getProjectValue(), table);
    }
    //todo schema
    coordinate.setPartitionSpec(partitionSpec);
    return coordinate;
  }

}
