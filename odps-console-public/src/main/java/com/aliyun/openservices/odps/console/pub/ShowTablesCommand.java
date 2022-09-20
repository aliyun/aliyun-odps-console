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

import java.io.PrintStream;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableFilter;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.CommandWithOptionP;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

/**
 * List tables in the project
 *
 * SHOW TABLES [IN project_name];
 *
 * @author <a
 *         href="shenggong.wang@alibaba-inc.com">shenggong.wang@alibaba-inc.com
 *         </a>
 */
public class ShowTablesCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "list", "ls", "table", "tables"};

  private static final String COORDINATE_GROUP_NAME = "coordinate";
  private static final String PREFIX_GROUP_NAME = "prefix";

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*SHOW\\s+TABLES"
      + "(\\s+IN\\s+(?<coordinate>[\\w.]+)|\\s*)"
      + "(\\s*|(\\s+LIKE\\s+'(?<prefix>\\w*)(\\*|%)'))\\s*",
      Pattern.CASE_INSENSITIVE);

  private static final Pattern PUBLIC_PATTERN = Pattern.compile(
          "\\s*(LS|LIST)\\s+TABLES.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private Coordinate coordinate;
  private final String prefix;

  public ShowTablesCommand(String cmd, ExecutionContext cxt, Coordinate coordinate, String prefix) {
    super(cmd, cxt);
    this.coordinate = coordinate;
    this.prefix = prefix;
  }

  public static void printUsage(PrintStream stream, ExecutionContext ctx) {
    stream.println("Usage:");
    if (ctx.isProjectMode()) {
      stream.println("  show tables [in <project name>] [like '<prefix>']");
      stream.println("Examples:");
      stream.println("  show tables;");
      stream.println("  show tables like my_%;");
      stream.println("  show tables in my_project;");
      stream.println("  show tables in my_project.my_schema;");
    } else {
      stream.println("  show tables [in [<project name>.]<schema name>] [like '<prefix>']");
      stream.println("Examples:");
      stream.println("  show tables;");
      stream.println("  show tables like my_%;");
      stream.println("  show tables in my_schema;");
      stream.println("  show tables in my_project.my_schema;");

    }
  }

  @Override
  public void run() throws ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String project = coordinate.getProjectName();
    String schema = coordinate.getSchemaName();

    DefaultOutputWriter writer = getContext().getOutputWriter();

    Odps odps = getCurrentOdps();

    TableFilter prefixFilter = new TableFilter();
    prefixFilter.setName(prefix);

    Iterator<Table> it = odps.tables().iterator(project, schema, prefixFilter, false);

    writer.writeResult("");// for HiveUT

    while (it.hasNext()) {
      ODPSConsoleUtils.checkThreadInterrupted();
      Table table = it.next();
      writer.writeResult(table.getOwner() + ":" + table.getName());
    }

    // TODO: time taken & fetched rows
    writer.writeError("\nOK");
  }

  // for chain
  public static ShowTablesCommand parse(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {

    // 1. match list tables -p
    Coordinate coordinate = parseListTablesCommand(cmd, cxt);
    String prefixName = null;
    if (coordinate == null) {
      // 2. match show tables
      Matcher showMatcher = PATTERN.matcher(cmd);
      if (!showMatcher.matches()) {
        return null;
      }

      // 3. parse
      prefixName = showMatcher.group(PREFIX_GROUP_NAME);
      coordinate = Coordinate.getCoordinateAB(showMatcher.group(COORDINATE_GROUP_NAME));
    }

    return new ShowTablesCommand(cmd, cxt, coordinate, prefixName);
  }

  public static Coordinate parseListTablesCommand(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {
    Matcher matcher = PUBLIC_PATTERN.matcher(cmd);
    if (!matcher.matches()) {
      return null;
    }

    CommandWithOptionP command = new CommandWithOptionP(cmd);
    if (command.getArgs().length != 2) {
      throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
    }

    String project = command.getProjectValue();
    return Coordinate.getCoordinateOptionP(project, "null");
  }

}
