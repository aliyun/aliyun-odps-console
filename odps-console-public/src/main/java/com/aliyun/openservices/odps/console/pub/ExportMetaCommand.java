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

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.Table;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

import java.util.Iterator;

/**
 * 只为金融升级odpscmd用
 * 
 * @author shuman.gansm
 * */
public class ExportMetaCommand extends AbstractCommand {

  final static String SHOW_PARTITIONS = "SHOW_PARTITIONS";
  final static String SHOW_TABLES = "SHOW_TABLES";
  final static String DESC_TABLES = "DESC_TABLES";

  private String commandMark = "";
  private String tableName = "";

  public String getTableName() {
    return tableName;
  }

  public String getCommandMark() {
    return commandMark;
  }

  public ExportMetaCommand(String commandText, ExecutionContext context, String tableName,
      String commandMark) {
    super(commandText, context);

    this.tableName = tableName;
    this.commandMark = commandMark;

  }

  public void run() throws OdpsException, ODPSConsoleException {

    String project = getCurrentProject();
    Odps odps = getCurrentOdps();

    // 调用sdk得到相应的内容
    if (SHOW_PARTITIONS.equals(commandMark)) {
      Table table = odps.tables().get(project, tableName);
      table.reload();
      Iterator<Partition> partitionList = table.getPartitionIterator();

      StringBuilder builder = new StringBuilder();
      builder.append("[");
      for (; partitionList.hasNext();) {
        Partition partition = partitionList.next();
        String partition_spec = partition.getPartitionSpec().toString();
        builder.append(partition_spec.replace('\'', '"'));

        if (partitionList.hasNext()) {
          builder.append(",\n");
        }
      }

      builder.append("]");

      System.out.println(builder.toString());
    } else if (SHOW_TABLES.equals(commandMark)) {
      Iterator<Table> tableList = odps.tables().iterator(project);

      StringBuilder builder = new StringBuilder();
      builder.append("[");
      for (; tableList.hasNext();) {
        Table info = tableList.next();

        builder.append("\"" + info.getOwner() + ":" + info.getName() + "\"");
        if (tableList.hasNext()) {
          builder.append(",\n");
        }
      }
      builder.append("]");

      System.out.println(builder.toString());

    } else if (DESC_TABLES.equals(commandMark)) {
      String[] tableSpec = ODPSConsoleUtils.parseTableSpec(tableName);
      String projectName = tableSpec[0];

      if (projectName == null) {
        projectName = project;
      }
      String realTableName = tableSpec[1];

      Table table = odps.tables().get(projectName, realTableName);
      table.reload();

      System.out.println(table.getJsonSchema());
    }
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static ExportMetaCommand parse(String commandString, ExecutionContext sessionContext) {

    // 只有金融模式会处理相关信息
    if (sessionContext.isJson()) {

      String[] splits = commandString.trim().split(" ");

      // 为金融导出meta使用
      if (commandString.toUpperCase().matches("\\s*SHOW\\s+PARTITIONS.*") && splits.length == 3) {

        return new ExportMetaCommand(commandString, sessionContext, splits[2], SHOW_PARTITIONS);
      }

      if (commandString.toUpperCase().matches("\\s*SHOW\\s+TABLES.*") && splits.length == 2) {

        return new ExportMetaCommand(commandString, sessionContext, "", SHOW_TABLES);
      }

      if (commandString.toUpperCase().matches("\\s*DESC.*") && splits.length == 2) {

        return new ExportMetaCommand(commandString, sessionContext, splits[1], DESC_TABLES);
      }
    }

    return null;
  }
}
