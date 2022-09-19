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

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Table;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.ExportProjectUtil;

import java.io.PrintStream;

public class ExportTableCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"export", "table"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: export table [<project name>.[<schema name>]]<tablename>");
  }

  private Coordinate coordinate;

  public ExportTableCommand(Coordinate coordinate,
                        String commandText, ExecutionContext context ) {
    super(commandText, context);
    this.coordinate = coordinate;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());

    Table table = getCurrentOdps().tables().get(coordinate.getProjectName(),
                                                coordinate.getSchemaName(),
                                                coordinate.getObjectName());

    getWriter().writeResult(
        "DDL:"
        + ExportProjectUtil.getDdlFromMeta(getCurrentOdps(), table, "-tp"));

  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static ExportTableCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    if (commandString.toUpperCase().matches("\\s*EXPORT\\s+TABLE.*")) {

      String temp[] = commandString.trim().split("\\s+");

      if (temp.length == 3) {
        Coordinate coordinate = Coordinate.getCoordinateABC(temp[2]);
        return new ExportTableCommand(coordinate, commandString, sessionContext);
      }

      // export 命令可以给sql去执行，由sql来抛错
    }

    return null;
  }

}
