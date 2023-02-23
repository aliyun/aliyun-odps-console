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

import com.aliyun.odps.Odps;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableFilter;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
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
public class ShowViewsCommand extends ShowTablesCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "views", "view", "materialized"};

  private static final String MATERIALIZED_GROUP_NAME = "materialized";
  private static final String COORDINATE_GROUP_NAME = "coordinate";
  private static final String PREFIX_GROUP_NAME = "prefix";

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*SHOW\\s+(((?<" + MATERIALIZED_GROUP_NAME + ">materialized)\\s+)?)VIEWS"
      + "(\\s+IN\\s+(?<coordinate>[\\w.]+)|\\s*)"
      + "(\\s*|(\\s+LIKE\\s+'(?<prefix>\\w*)(\\*|%)'))\\s*",
      Pattern.CASE_INSENSITIVE);

  public ShowViewsCommand(String cmd, ExecutionContext cxt, Coordinate coordinate, String prefix, Table.TableType type) {
    super(cmd, cxt, coordinate, prefix, type);
  }

  public static void printUsage(PrintStream stream, ExecutionContext ctx) {
    stream.println("Usage:");
    if (ctx.isProjectMode()) {
      stream.println("  show [materialized] views [in <project name>] [like '<prefix>']");
      stream.println("Examples:");
      stream.println("  show views;");
      stream.println("  show materialized views;");
      stream.println("  show views like my_%;");
      stream.println("  show views in my_project;");
      stream.println("  show views in my_project like my_%;");
      stream.println("  show materialized views in my_project;");
    } else {
      stream.println("  show views [in [<project name>.]<schema name>] [like '<prefix>']");
      stream.println("Examples:");
      stream.println("  show views;");
      stream.println("  show materialized views;");
      stream.println("  show views like my_%;");
      stream.println("  show views in my_schema;");
      stream.println("  show views in my_project.my_schema;");
      stream.println("  show materialized views in my_project;");
    }
  }

  @Override
  public void run() throws ODPSConsoleException {
    super.run();
  }

  // for chain
  public static ShowViewsCommand parse(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {

    String prefixName = null;
    boolean isMaterialized = false;

    Matcher showMatcher = PATTERN.matcher(cmd);
    if (!showMatcher.matches()) {
      return null;
    }
    prefixName = showMatcher.group(PREFIX_GROUP_NAME);
    isMaterialized = (showMatcher.group(MATERIALIZED_GROUP_NAME) != null) ;

    Coordinate coordinate = Coordinate.getCoordinateAB(showMatcher.group(COORDINATE_GROUP_NAME));

    return new ShowViewsCommand(cmd, cxt, coordinate, prefixName, isMaterialized ? Table.TableType.MATERIALIZED_VIEW : Table.TableType.VIRTUAL_VIEW);
  }

}
