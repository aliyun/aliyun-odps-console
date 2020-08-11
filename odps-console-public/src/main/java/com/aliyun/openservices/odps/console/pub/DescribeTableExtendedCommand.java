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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringEscapeUtils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ErrorCode;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.PluginUtil;

/**
 * Created by yinyue on 14-12-6.
 */

/**
 * Describe extended table meta
 *
 * DESCRIBE|DESC EXTENDED table_name [PARTITION(partition_col = 'parition_col_value', ...)]
 *
 */
public class DescribeTableExtendedCommand extends AbstractCommand {

  private String project;
  private String table;
  private String partition;

  private static List<String> reservedPrintFields = null;

  public static final String[] HELP_TAGS = new String[]{"describe", "desc", "extended"};

  static {
    try {
      if (reservedPrintFields == null) {
        Properties properties = PluginUtil.getPluginProperty(DescribeTableExtendedCommand.class);
        String cmd = properties.getProperty("reserved_print_fields");
        if (!StringUtils.isNullOrEmpty(cmd)) {
          reservedPrintFields = Arrays.asList(cmd.split(","));
        }
      }
    } catch (IOException e) {
      // a warning
      System.err.println("Warning: load config failed, cannot get table reserved print fields.");
      System.err.flush();
    }

  }

  // Each subtype of desc command is not visible in CommandParserUtils, so group all help info here together
  public static void printUsage(PrintStream stream) {
    stream
        .println("Usage: describe|desc [extended] [<projectname>.]<tablename> [partition(<spec>)]");
  }

  public DescribeTableExtendedCommand(String cmd, ExecutionContext cxt, String project,
                                      String table,
                                      String partition) {
    super(cmd, cxt);
    this.project = project;
    this.table = table;
    this.partition = partition;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.aliyun.openservices.odps.console.commands.AbstractCommand#run()
   */
  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    if (table == null || table.length() == 0) {
      throw new OdpsException(
          ErrorCode.INVALID_COMMAND
          + ": Invalid syntax - DESCRIBE|DESC [EXTENDED] table_name [PARTITION(partition_col = 'partition_col_value';");
    }

    DefaultOutputWriter writer = this.getContext().getOutputWriter();

    Odps odps = getCurrentOdps();

    if (project == null) {
      project = getCurrentProject();
    }

    Table t = odps.tables().get(project, table);

    writer.writeResult(""); // for HiveUT

    String result = "";
    if (partition == null) {
      t.reload();
      result = getScreenDisplay(t, null);
    } else {
      if (partition.trim().length() == 0) {
        throw new OdpsException(ErrorCode.INVALID_COMMAND + ": Invalid partition key.");
      }

      Partition meta = t.getPartition(new PartitionSpec(partition));
      meta.reload();
      result = getScreenDisplay(t, meta);
    }
    writer.writeResult(result);
    System.out.flush();

    writer.writeError("OK");
  }

  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+(EXTENDED)\\s+(.*)",
                                                   Pattern.CASE_INSENSITIVE);

  public static DescribeTableExtendedCommand parse(String cmd, ExecutionContext cxt) {
    if (cmd == null || cxt == null) {
      return null;
    }

    DescribeTableExtendedCommand r = null;
    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return r;
    }

    if (m.groupCount() == 3) {
      cmd = m.group(3);
    } else {
      return r;
    }

    ODPSConsoleUtils.TablePart tablePart = ODPSConsoleUtils.getTablePart(cmd);

    if (tablePart.tableName != null) {
      String[] tableSpec = ODPSConsoleUtils.parseTableSpec(tablePart.tableName);
      String project = tableSpec[0];
      String table = tableSpec[1];

      r = new DescribeTableExtendedCommand(cmd, cxt, project, table, tablePart.partitionSpec);
    }

    return r;
  }

  private void appendClusterInfo(Table.ClusterInfo clusterInfo, PrintWriter w) {
    if (!StringUtils.isNullOrEmpty(clusterInfo.getClusterType())) {
      w.printf("| ClusterType:              %-56s |\n", clusterInfo.getClusterType());
    }
    if (clusterInfo.getBucketNum() != -1) {
      w.printf("| BucketNum:                %-56s |\n", clusterInfo.getBucketNum());
    }
    if (clusterInfo.getClusterCols() != null && !clusterInfo.getClusterCols().isEmpty()) {
      String cols = Arrays.toString(clusterInfo.getClusterCols().toArray());
      w.printf("| ClusterColumns:           %-56s |\n", cols);
    }

    if (clusterInfo.getSortCols() != null && !clusterInfo.getSortCols().isEmpty()) {
      String cols = Arrays.toString(clusterInfo.getSortCols().toArray());
      w.printf("| SortColumns:              %-56s |\n", cols);
    }
  }

  // port from task/sql_task/query_result_helper.cpp:PrintTableMeta
  private String getScreenDisplay(Table t, Partition meta) throws ODPSConsoleException {

    String result = DescribeTableCommand.getScreenDisplay(t, meta, true);

    if (t.isVirtualView()) {
      return result;
    }

    StringWriter out = new StringWriter();
    PrintWriter w = new PrintWriter(out);

    try {
      if (meta != null) {
        if (meta.getLifeCycle() != -1) {
          w.printf("| LifeCycle:                %-56s |\n", meta.getLifeCycle());
        }
        w.printf("| IsExstore:                %-56s |\n", meta.isExstore());
        w.printf("| IsArchived:               %-56s |\n", meta.isArchived());
        w.printf("| PhysicalSize:             %-56s |\n", meta.getPhysicalSize());
        w.printf("| FileNum:                  %-56s |\n", meta.getFileNum());
        if (meta.getClusterInfo() != null) {
          appendClusterInfo(meta.getClusterInfo(), w);
        }
        w.println(
            "+------------------------------------------------------------------------------------+");
      } else {
        w.println(
            "| Extended Info:                                                                     |");
        w.println(
            "+------------------------------------------------------------------------------------+");

        if (t.isExternalTable()) {
          if (!StringUtils.isNullOrEmpty(t.getTableID())) {
            w.printf("| %-20s: %-60s |\n", "TableID", t.getTableID());
          }
          if (!StringUtils.isNullOrEmpty(t.getStorageHandler())) {
            w.printf("| %-20s: %-60s |\n", "StorageHandler", t.getStorageHandler());
          }

          if (!StringUtils.isNullOrEmpty(t.getLocation())) {
            w.printf("| %-20s: %-60s |\n", "Location", t.getLocation());
          }

          if (!StringUtils.isNullOrEmpty(t.getResources())) {
            w.printf("| %-20s: %-60s |\n", "Resources", t.getResources());

          }

          if (t.getSerDeProperties() != null) {
            for (Map.Entry<String, String> entry : t.getSerDeProperties().entrySet()) {
              w.printf("| %-20s: %-60s |\n", entry.getKey(),
                       StringEscapeUtils.escapeJava(
                           entry.getValue()));
            }

          }

        } else {
          if (!StringUtils.isNullOrEmpty(t.getTableID())) {
            w.printf("| TableID:                  %-56s |\n", t.getTableID());
          }
          w.printf("| IsArchived:               %-56s |\n", t.isArchived());
          w.printf("| PhysicalSize:             %-56s |\n", t.getPhysicalSize());
          w.printf("| FileNum:                  %-56s |\n", t.getFileNum());
        }

        if (!CollectionUtils.isEmpty(reservedPrintFields) && !StringUtils
            .isNullOrEmpty(t.getReserved())) {
          JsonObject object = new JsonParser().parse(t.getReserved()).getAsJsonObject();
          for (String key : reservedPrintFields) {
            if (object.has(key)) {
              int spaceLength = Math.max((25 - key.length()), 1);
              w.printf(String.format("| %s:%-" + spaceLength + "s%-56s |\n", key, " ",
                                     object.get(key).getAsString()));
            }
          }
        }

        if (!StringUtils.isNullOrEmpty(t.getCryptoAlgoName())) {
          w.printf("| CryptoAlgoName:           %-56s |\n", t.getCryptoAlgoName());
        }

        if (t.getClusterInfo() != null) {
          appendClusterInfo(t.getClusterInfo(), w);
        }
        w.println(
            "+------------------------------------------------------------------------------------+");
      }
    } catch (Exception e) {
      throw new ODPSConsoleException(ErrorCode.INVALID_RESPONSE + ": Invalid table schema.", e);
    }

    w.flush();
    w.close();

    return result + out.toString();
  }
}
