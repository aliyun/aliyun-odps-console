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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.data.ArrowStreamRecordReader;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.converter.OdpsRecordConverter;
import com.aliyun.odps.data.converter.OdpsRecordConverterBuilder;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import org.apache.commons.lang.BooleanUtils;

import static com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants.ODPS_READ_LEGACY;

/**
 * @author shuman.gansm
 */
public class ReadTableCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"read", "table"};

  public static void printUsage(PrintStream stream, ExecutionContext ctx) {
    if (ctx.isProjectMode()) {
      stream.println("Usage: read [<project_name>.]<table_name> [(<col_name>[,..])]"
                     + " [PARTITION (<partition_spec>)] [line_num]");
    } else {
      stream.println("Usage: read [[<project_name>.]<schema_name>.]<table_name>"
                     + " [(<col_name>[,..])] [PARTITION (<partition_spec>)] [line_num]");
    }
  }

  private Coordinate coordinate;
  private Integer lineNum;
  private List<String> columns;
  private boolean useLegacyType;
  private OdpsRecordConverter formatter;

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  public ReadTableCommand(Coordinate coordinate,
                          List<String> columns,
                          int lineNum,
                          String commandText,
                          ExecutionContext context) {
    super(commandText, context);
    this.coordinate = coordinate;
    this.columns = columns;
    this.lineNum = lineNum;
    useLegacyType = BooleanUtils.toBoolean(SetCommand.setMap.getOrDefault(ODPS_READ_LEGACY, "true"));
    OdpsRecordConverterBuilder formatterBuilder =
        OdpsRecordConverter.builder().complexFormatHumanReadable().enableParseNull();
    if (useLegacyType) {
      formatterBuilder = formatterBuilder.useLegacyTimeType().floatingNumberFormatCompatible();
    }
    formatter = formatterBuilder.build();
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String projectName = coordinate.getProjectName();
    String schemaName = coordinate.getSchemaName();
    String tableName = coordinate.getObjectName();
    String partitionSpec = coordinate.getPartitionSpec();

    Odps odps = getCurrentOdps();

    // get cvs data
    if (getContext().isMachineReadable()) {
      String cvsStr;
      try {
        cvsStr = readCvsData(projectName, schemaName, tableName, partitionSpec, columns, lineNum);
        getWriter().writeResult(cvsStr);
        return;
      } catch (IOException e) {
        throw new OdpsException(e.getMessage(), e);
      }
    }
    Table table = odps.tables().get(projectName, schemaName, tableName);

    PartitionSpec spec = null;
    if (partitionSpec != null && partitionSpec.trim().length() > 0) {
      spec = new PartitionSpec(partitionSpec);
    }
    ArrowStreamRecordReader reader =
        (ArrowStreamRecordReader) table.read(spec, columns, lineNum, getContext().getSqlTimezone(),
                                             useLegacyType, getContext().getTunnelEndpoint());
    Map<String, TypeInfo>
        columnNameTypeMap =
        Arrays.stream(reader.getSchema()).collect(Collectors.toMap(Column::getName, Column::getTypeInfo));

    // get header
    Map<String, Integer> displayWidth = ODPSConsoleUtils.getDisplayWidth(
        table.getSchema().getColumns(),
        table.getSchema().getPartitionColumns(),
        columns);

    Record record;
    try {
      if (columns == null) {
        columns = Arrays.stream(reader.getSchema()).map(Column::getName).collect(Collectors.toList());
      }
      String frame = ODPSConsoleUtils.makeOutputFrame(displayWidth).trim();
      String title =
          ODPSConsoleUtils.makeTitleByString(columns, displayWidth).trim();
      getWriter().writeResult(frame);
      getWriter().writeResult(title);
      getWriter().writeResult(frame);

      while ((record = reader.read()) != null) {
        StringBuilder resultBuf = new StringBuilder();
        resultBuf.append("| ");
        Iterator<Integer> it = displayWidth.values().iterator();
        for (String column : columns) {
          String str =
              Optional.ofNullable(record.get(column))
                  .map(o -> formatter.formatObject(o, columnNameTypeMap.get(column)))
                  .orElse("NULL");
          resultBuf.append(str);
          int length = it.next();
          if (str.length() < length) {
            for (int j = 0; j < length - str.length(); j++) {
              resultBuf.append(" ");
            }
          }
          resultBuf.append(" | ");
        }
        getWriter().writeResult(resultBuf.toString().trim());
      }
      getWriter().writeResult(frame);
    } catch (Exception e) {
      throw new OdpsException(e.getMessage(), e);
    } finally {
      reader.close();
    }
  }

  private String readCvsData(
      String projectName,
      String schemaName,
      String tableName,
      String partition,
      List<String> columns,
      int top) throws OdpsException, ODPSConsoleException, IOException {

    PartitionSpec spec = null;
    if (partition != null && partition.length() != 0) {
      spec = new PartitionSpec(partition);
    }
    Odps odps = getCurrentOdps();
    ArrowStreamRecordReader reader = (ArrowStreamRecordReader) odps
        .tables().get(projectName, schemaName, tableName)
        .read(spec, columns, top, getContext().getSqlTimezone(), useLegacyType, getContext().getTunnelEndpoint());
    Map<String, TypeInfo>
        columnNameTypeMap =
        Arrays.stream(reader.getSchema()).collect(Collectors.toMap(Column::getName, Column::getTypeInfo));
    StringBuilder csv = new StringBuilder();
    if (columns == null) {
      columns = Arrays.stream(reader.getSchema()).map(Column::getName).collect(Collectors.toList());
    }
    String columnLine = String.join(",", columns);
    csv.append(columnLine).append("\n");
    Record record;
    while ((record = reader.read()) != null) {
      for (String column : columns) {
        csv.append(
            Optional.ofNullable(record.get(column))
                .map(o -> formatter.formatObject(o, columnNameTypeMap.get(column)))
                .orElse("NULL")).append(",");
      }
      csv.deleteCharAt(csv.length() - 1);
      csv.append("\n");
    }
    reader.close();
    return csv.toString();
  }

  public static ReadTableCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    String readCommandString = commandString;
    if (readCommandString.toUpperCase().matches("\\s*READ\\s+\\w[\\s\\S]*")) {

      // 会把所有多个空格都替换掉
      readCommandString = readCommandString.replaceAll("\\s+", " ");

      // remove "read "
      readCommandString = readCommandString.substring("read ".length()).trim();

      String tableName = "";
      int index = readCommandString.indexOf("(");
      if (index > 0) {
        tableName = readCommandString.substring(0, index);
        if (tableName.toUpperCase().indexOf(" PARTITION") > 0) {
          // tableName中也会包含PARTITION字符，
          tableName = tableName.substring(0, tableName.toUpperCase().indexOf(" PARTITION"));
        }
      } else {
        // 没有column和PARTITION，用" "空格来区分
        if (readCommandString.indexOf(" ") > 0) {
          tableName = readCommandString.substring(0, readCommandString.indexOf(" "));
        } else {
          // read mytable的情况
          tableName = readCommandString;
        }
      }

      // remove tablename, 把前面的空格也删除掉
      readCommandString = readCommandString.substring(tableName.length()).trim();

      String columns = "";
      if (readCommandString.startsWith("(") && readCommandString.indexOf(")") > 0) {
        // 取columns
        columns = readCommandString.substring(0, readCommandString.indexOf(")") + 1);
      }
      // remove columns, 把前面的空格也删除掉
      readCommandString = readCommandString.substring(columns.length()).trim();

      String partitions = "";
      if (readCommandString.toUpperCase().indexOf("PARTITION") == 0
          && readCommandString.indexOf("(") > 0 && readCommandString.indexOf(")") > 0
          && readCommandString.indexOf("(") < readCommandString.indexOf(")")) {

        partitions = readCommandString.substring(readCommandString.indexOf("("),
                                                 readCommandString.indexOf(")") + 1);
        readCommandString = readCommandString.substring(readCommandString.indexOf(")") + 1).trim();
      }

      // 默认 10W > 服务器端返回10000行
      int lineNum = 100000;
      if (!"".equals(readCommandString)) {
        try {
          lineNum = Integer.parseInt(readCommandString);
        } catch (NumberFormatException e) {
          // 最后只剩下lineNum，如果转成 linenum出错，则命令出错
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
        }
      }

      tableName = tableName.trim();
      Coordinate coordinate = Coordinate.getCoordinateABC(tableName);
      coordinate.setPartitionSpec(populatePartitions(partitions));

      List<String> columnList = validateAndGetColumnList(columns);

      return new ReadTableCommand(coordinate, columnList, lineNum, commandString, sessionContext);
    }

    return null;
  }

  private static List<String> validateAndGetColumnList(String columns) throws ODPSConsoleException {
    columns = columns.replace("(", "").replace(")", "")
        .toLowerCase().trim();

    if (columns.isEmpty()) {
      return null;
    }

    String[] columnArray = columns.split(",");
    for (int i = 0; i < columnArray.length; i++) {
      columnArray[i] = columnArray[i].trim();

      // column不能出现空格
      if (columnArray[i].contains(" ")) {
        throw new ODPSConsoleException(ODPSConsoleConstants.COLUMNS_ERROR);
      }
    }

    return Arrays.asList(columnArray);
  }

  private static String populatePartitions(String partitions) throws ODPSConsoleException {
    // 转成partition_spc
    String partitionSpec = partitions.replace("(", "")
        .replace(")", "").trim();

    // 需要trim掉前后的空格，但不能删除掉partition_value中的空格
    if (partitionSpec.isEmpty()) {
      return "";
    }

    try {
      PartitionSpec spec = new PartitionSpec(partitionSpec);
      return spec.toString();
    } catch (Exception e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.PARTITION_SPC_ERROR);
    }
  }

  public static void main(String[] args) throws ODPSConsoleException {
    ExecutionContext ctx = new ExecutionContext();
    ctx.setProjectName("a");
    ReadTableCommand cmd = ReadTableCommand.parse("read b", ctx);
  }

}
