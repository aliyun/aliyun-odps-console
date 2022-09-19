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

package com.aliyun.openservices.odps.console.tunnel;

import com.aliyun.odps.ship.DShipCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import java.io.File;
import java.io.PrintStream;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;

/**
 * @author shuman.gansm
 * */
//TODO remove odps-console-tunnel
public class TunnelCommand extends AbstractCommand {

  private static final String DSHIP_COMMAND = "tunnel";
  private static final String DSHIP_SUBCOMMAND_UPLOAD = "upload";
  private static final String DSHIP_SUBCOMMAND_DOWNLOAD = "download";
  private static final String DSHIP_OPTION_CHARSET = "-c";
  private static final String DSHIP_OPTION_DATETIME_FORMAT_PATTERN = "-dfp";
  private static final String DSHIP_OPTION_NULL_INDICATOR = "-ni";
  private static final String DSHIP_OPTION_FIELD_DELIMITER = "-fd";
  private static final String DSHIP_OPTION_RECORD_DELIMITER = "-rd";
  private static final String DSHIP_OPTION_DISCARD_BAD_RECORDS = "-dbr";

  public static void printUsage(PrintStream out) {
    out.println("Usage: upload <table>[ partition(<partition spec>)] from <path>");
    out.println("Usage: download <table>[ partition(<partition spec>)] to <path>");
  }

  public TunnelCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
    System.err.println("Tunnel command is deprecated, please use dship command");
  }

  public TunnelCommand(String tableName, String filename, boolean isUp, String commandText,
      ExecutionContext context) {
    super(commandText, context);
    System.err.println("Tunnel command is deprecated, please use dship command");
  }

  public void run() throws OdpsException, ODPSConsoleException {
    System.err.println("Tunnel command is deprecated, please use dship command");
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static TunnelCommand parse(List<String> optionList, ExecutionContext sessionContext) {
    return null;
  }

  public static DShipCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    String readCommandString = commandString;
    if (readCommandString.toUpperCase().matches("\\s*UPLOAD\\s+\\w[\\s\\S]*")
        || readCommandString.toUpperCase().matches("\\s*DOWNLOAD\\s+\\w[\\s\\S]*")) {
      // 会把所有多个空格都替换掉
      readCommandString = readCommandString.replaceAll("\\s+", " ");

      boolean isUp = true;

      if (readCommandString.toUpperCase().matches("\\s*DOWNLOAD\\s+\\w[\\s\\S]*")) {
        readCommandString = readCommandString.substring("DOWNLOAD ".length()).trim();
        isUp = false;
      } else {
        readCommandString = readCommandString.substring("UPLOAD ".length()).trim();
      }

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

      String partitions = "";
      if (readCommandString.toUpperCase().indexOf("PARTITION") == 0
          && readCommandString.indexOf("(") > 0 && readCommandString.indexOf(")") > 0
          && readCommandString.indexOf("(") < readCommandString.indexOf(")")) {

        partitions = readCommandString.substring(readCommandString.indexOf("("),
            readCommandString.indexOf(")") + 1);
        // remove partitions
        readCommandString = readCommandString.substring(readCommandString.indexOf(")") + 1).trim();
      }

      String filename = "";
      if (isUp && readCommandString.toUpperCase().startsWith("FROM ")) {
        filename = readCommandString.substring(readCommandString.indexOf(" "));
      } else if (!isUp && readCommandString.toUpperCase().startsWith("TO ")) {
        filename = readCommandString.substring(readCommandString.indexOf(" "));
      } else {
        throw new ODPSConsoleException("bad command. '" + readCommandString + "'");
      }
      filename = filename.trim();

      readCommandString = readCommandString.substring(readCommandString.indexOf(filename)
          + filename.length());

      if (!"".equals(readCommandString)) {
        // TODO help
        throw new ODPSConsoleException("bad command. '" + readCommandString + "'");
      }

      // 转成partition_spec
      partitions = partitions.replace("(", "").replace(")", "")
                             .trim();

      // Replace tunnel command with dship command to get rid of the old tunnel-sdk
      String dshipCommandText = transformIntoDshipCommand(tableName, partitions, filename, isUp);

      DefaultOutputWriter writer = sessionContext.getOutputWriter();
      writer.writeDebug("Replacing command: " + commandString);
      writer.writeDebug("with " + dshipCommandText);

      return new DShipCommand(dshipCommandText, sessionContext);
    }

    return null;
  }

  private static String transformIntoDshipCommand(String tableName, String partitionSpec,
                                                  String filename, boolean isUpload)
                                                  throws ODPSConsoleException {
    StringBuilder dshipCommandBuilder = new StringBuilder();
    dshipCommandBuilder.append(DSHIP_COMMAND);
    dshipCommandBuilder.append(" ");
    if (isUpload) {
      dshipCommandBuilder.append(DSHIP_SUBCOMMAND_UPLOAD);
    } else {
      dshipCommandBuilder.append(DSHIP_SUBCOMMAND_DOWNLOAD);
    }
    dshipCommandBuilder.append(" ");

    // Handle tunnel command configurations
    Config config = Config.getConfig();
    String charset = config.getCharset();
    String datetimeFormat = config.getDateFormat();
    Character colDelimiter = config.getColDelimiter();
    Character rowDelimiter = config.getRowDelimiter();
    String nullIndicator = config.getNullIndicator();
    dshipCommandBuilder.append(DSHIP_OPTION_CHARSET);
    dshipCommandBuilder.append(formatArgument(charset));
    dshipCommandBuilder.append(DSHIP_OPTION_DATETIME_FORMAT_PATTERN);
    dshipCommandBuilder.append(formatArgument(datetimeFormat));
    dshipCommandBuilder.append(DSHIP_OPTION_FIELD_DELIMITER);
    dshipCommandBuilder.append(formatArgument(colDelimiter));
    dshipCommandBuilder.append(DSHIP_OPTION_RECORD_DELIMITER);
    dshipCommandBuilder.append(formatArgument(rowDelimiter));
    dshipCommandBuilder.append(DSHIP_OPTION_NULL_INDICATOR);
    dshipCommandBuilder.append(formatArgument(nullIndicator));
    if (isUpload) {
      Boolean discardBadRecord = config.isBadDiscard();
      dshipCommandBuilder.append(DSHIP_OPTION_DISCARD_BAD_RECORDS);
      dshipCommandBuilder.append(formatArgument(discardBadRecord.toString()));

      Long maxSize = config.getMaxSize();
      // Check if max size is violated
      File fileToUpload = new File(filename);
      if (fileToUpload.exists() && fileToUpload.length() > maxSize) {
        throw new ODPSConsoleException("file size exceed " + maxSize / 1024 / 1024 + "M");
      } else if (!fileToUpload.exists()) {
        throw new ODPSConsoleException("file not found.");
      }
    }

    // Handle table name, partition spec and file name
    String tableAndPartition = tableName;
    if (!StringUtils.isNullOrEmpty(partitionSpec)) {
      tableAndPartition += "/" + partitionSpec;
    }
    if (isUpload) {
      dshipCommandBuilder.append(filename);
      dshipCommandBuilder.append(" ");
      dshipCommandBuilder.append(tableAndPartition);
    } else {
      dshipCommandBuilder.append(tableAndPartition);
      dshipCommandBuilder.append(" ");
      dshipCommandBuilder.append(filename);
    }

    return dshipCommandBuilder.toString();
  }

  private static String formatArgument(String argument) {
    return " \'" + argument + "\' ";
  }

  private static String formatArgument(Character argument) {
    return " \'" + argument + "\' ";
  }
}
