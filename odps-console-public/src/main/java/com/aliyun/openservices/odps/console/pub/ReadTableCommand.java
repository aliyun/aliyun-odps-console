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
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.data.DefaultRecordReader;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

/**
 * @author shuman.gansm
 * */
public class ReadTableCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"read", "table"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: read  <table_name> [<(col_name>[,..])][PARTITION <(partition_spec)>][line_num]");
  }

  private String tableName;
  private Integer lineNum;
  private String partitions = "";
  private List<String> columns;
  // 在read时可以加上project.tablename
  String refProjectName = null;

  public String getRefProjectName() {
    return refProjectName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Integer getLineNum() {
    return lineNum;
  }

  public void setLineNum(Integer lineNum) {
    this.lineNum = lineNum;
  }

  public String getPartitions() {
    return partitions;
  }

  public void setPartitions(String partitions) {
    this.partitions = partitions;
  }

  public List<String> getColumns() {
    return columns;
  }

  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  public ReadTableCommand(String tableName, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.tableName = tableName;

  }

  public void run() throws OdpsException, ODPSConsoleException {

    // String prjName = "";
    if (refProjectName == null) {
      refProjectName = getContext().getProjectName();
    }

    if (refProjectName == null || refProjectName.trim().equals("")) {
      throw new OdpsException(ODPSConsoleConstants.PROJECT_NOT_BE_SET);
    }

    Odps odps = getCurrentOdps();

    // get cvs data
    if (getContext().isMachineReadable()) {

      String cvsStr = "";
      cvsStr = readCvsData(refProjectName, tableName, partitions, columns, lineNum);
      getWriter().writeResult(cvsStr);
      return;
    }
    Table table = odps.tables().get(refProjectName, tableName);

    // 通过获取table的meta去得到相应的显示宽度

    PartitionSpec spec = null;
    if (partitions != null && partitions.trim().length() > 0) {
      spec = new PartitionSpec(partitions);
    }

    DefaultRecordReader reader = (DefaultRecordReader) table.read(spec, columns, lineNum);

    // get header
    Map<String, Integer> displayWith = getDisplayWith(table.getSchema());
    List<String> nextLine;
    try {
      String frame = makeFrame(displayWith).trim();
      String title = makeTitle(reader, displayWith).trim();
      getWriter().writeResult(frame);
      getWriter().writeResult(title);
      getWriter().writeResult(frame);
      while ((nextLine = reader.readRaw()) != null) {
        StringBuilder resultBuf = new StringBuilder();
        resultBuf.append("| ");
        Iterator<Integer> it = displayWith.values().iterator();
        for (int i = 0; i < nextLine.size(); ++i) {
          String str;
          str = nextLine.get(i);
          // sdk的opencsv的库读""空串时有一个bug，可能读出来是"
          // 这也会带来一个新的问题，但字段出现空的概率比较大，先不管"号的情况
          if (str == null) {
            str = "NULL";
          }
          if ("\"".equals(str)) {
            str = "";
          }
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

    } catch (IOException e) {
      throw new OdpsException(e.getMessage(), e);
    }
  }

  private String makeTitle(DefaultRecordReader reader, Map<String, Integer> displayWith)
      throws IOException {
    StringBuilder titleBuf = new StringBuilder();
    titleBuf.append("| ");
    for (int i = 0; i < reader.getSchema().length; ++i) {
      String str = reader.getSchema()[i].getName();
      titleBuf.append(str);
      if (str.length() < displayWith.get(str)) {
        for (int j = 0; j < displayWith.get(str) - str.length(); j++) {
          titleBuf.append(" ");
        }
      }
      titleBuf.append(" | ");
    }
    return titleBuf.toString();

  }

  private String makeFrame(Map<String, Integer> displayWith) {
    StringBuilder sb = new StringBuilder();
    sb.append("+");
    for (Entry entry : displayWith.entrySet()) {
      for (int i = 0; i < (Integer) entry.getValue() + 2; i++)
        sb.append("-");
      sb.append("+");
    }
    return sb.toString();

  }

  private Map<String, Integer> getDisplayWith(TableSchema tableSchema) throws ODPSConsoleException {
    Map<String, Integer> displayWith = new LinkedHashMap<String, Integer>();
    Map<String, OdpsType> fieldTypeMap = new LinkedHashMap<String, OdpsType>();
    Map<String, OdpsType> partitionTypeMap = new LinkedHashMap<String, OdpsType>();
    // Get Column Info from Table Meta
    List<Column> columnArray = tableSchema.getColumns();
    for (int i = 0; i < columnArray.size(); i++) {
      Column column = columnArray.get(i);
      fieldTypeMap.put(column.getName(), column.getType());
    }
    // Get Partition Info from Table Meta
    List<Column> partitionArray = tableSchema.getPartitionColumns();
    for (int i = 0; i < partitionArray.size(); i++) {
      Column partition = partitionArray.get(i);
      partitionTypeMap.put(partition.getName(), partition.getType());
    }
    // delete the column which is not in columns
    if (columns != null && columns.size() != 0) {
      Set<String> set = fieldTypeMap.keySet();
      List<String> remainList = new ArrayList<String>(set);
      Iterator<String> it = set.iterator();
      while (it.hasNext()) {
        Object o = it.next();
        if (columns.contains(o))
          remainList.remove(o);
      }

      for (Object o : remainList) {
        fieldTypeMap.remove(o);
      }
    }
    // According the fieldType to calculate the display width
    for (Entry entry : fieldTypeMap.entrySet()) {
      if (entry.getValue().toString().toUpperCase().equals("BOOLEAN"))
        displayWith.put(entry.getKey().toString(), entry.getKey().toString().length() > 4 ? entry
            .getKey().toString().length() : 4);
      else
        displayWith.put(entry.getKey().toString(), entry.getKey().toString().length() > 10 ? entry
            .getKey().toString().length() : 10);
    }
    for (Entry entry : partitionTypeMap.entrySet()) {
      displayWith.put(entry.getKey().toString(), entry.getKey().toString().length() > 10 ? entry
          .getKey().toString().length() : 10);
    }
    return displayWith;

  }
  
  private String readCvsData(String refProjectName, String tableName, String partition,
      List<String> columns, int top) throws OdpsException, ODPSConsoleException {
  
    PartitionSpec spec = null;
    if (partition != null && partition.length() != 0) {
       spec = new PartitionSpec(partition);
    }
    Odps odps = getCurrentOdps();
    DefaultRecordReader response = (DefaultRecordReader) odps.tables().get(refProjectName, tableName).read(spec, columns, top);
    InputStream content = response.getRawStream();
    
    String cvsStr = "";
  
    try {
      cvsStr = IOUtils.readStreamAsString(content, "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new ODPSConsoleException(e.getMessage());
    } catch (IOException e) {
      throw new ODPSConsoleException(e.getMessage(), e);
    } finally {
      try {
        content.close();
      } catch (IOException e) {
      }
    }
  
    return cvsStr;
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
        // remove partitions
        readCommandString = readCommandString.substring(readCommandString.indexOf(")") + 1).trim();
      }

      // 默认 10W > 服务器端返回10000行
      int lineNum = 100000;
      if (!readCommandString.equals("")) {

        try {
          lineNum = Integer.valueOf(readCommandString);
        } catch (NumberFormatException e) {
          // 最后只剩下lineNum，如果转成 linenum出错，则命令出错
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
        }
      }

      tableName = tableName.trim();

      String pName = null;
      int prgIndex = tableName.indexOf(".");
      if (prgIndex > 0) {
        pName = tableName.substring(0, prgIndex);
        tableName = tableName.substring(prgIndex + 1);
      }

      ReadTableCommand command = new ReadTableCommand(tableName, commandString, sessionContext);
      if (pName != null) {
        command.refProjectName = pName;
      }

      // 转成partition_spc
      partitions = partitions.replace("(", "").replace(")", "").trim();
      // 需要trim掉前后的空格，但不能删除掉partition_value中的空格

      partitions = poplulatePartitions(partitions);
      command.setPartitions(partitions);

      columns = columns.replace("(", "").replace(")", "").toLowerCase().trim();
      validateColumns(columns);
      // 转成list
      if (columns != null && !"".equals(columns)) {

        columns = columns.replaceAll(" ", "");
        command.setColumns(Arrays.asList(columns.split(",")));
      }

      command.setLineNum(lineNum);

      return command;
    }

    return null;
  }

  private static void validateColumns(String columns) throws ODPSConsoleException {

    String[] columnArray = columns.split(",");

    for (int i = 0; i < columnArray.length; i++) {
      String column = columnArray[i].trim();

      // column不能出现空格
      if (column.indexOf(" ") > 0) {
        throw new ODPSConsoleException(ODPSConsoleConstants.COLUMNS_ERROR);
      }

    }
  }

  private static String poplulatePartitions(String partitionSpc) throws ODPSConsoleException {

    if ("".equals(partitionSpc)) {
      return "";
    }
    String result = "";
    String[] partitionArray = partitionSpc.split(",");

    try {
      for (int i = 0; i < partitionArray.length; i++) {
        String[] pA = partitionArray[i].split("=");
        String pKey = pA[0].trim();
        String pValue = pA[1].trim();

        if (!(pValue.startsWith("'") && pValue.endsWith("'") || pValue.startsWith("\"")
            && pValue.endsWith("\""))) {

          throw new ODPSConsoleException(ODPSConsoleConstants.PARTITION_SPC_ERROR);
        }

        if (i > 0)
          result += ",";

        result += pKey + "=" + pValue;

      }
    } catch (Exception e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.PARTITION_SPC_ERROR);
    }

    return result;

  }

}
