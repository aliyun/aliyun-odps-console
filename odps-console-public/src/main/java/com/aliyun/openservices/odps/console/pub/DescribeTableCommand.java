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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringEscapeUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.StorageTierInfo;
import com.aliyun.odps.StorageTierInfo.StorageTier;
import com.aliyun.odps.Table;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ErrorCode;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.common.CommandUtils;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.PluginUtil;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Describe table meta
 */
public class DescribeTableCommand extends AbstractCommand {

  private Coordinate coordinate;
  private boolean isExtended;

  private static List<String> reservedPrintFields = null;

  public static final String[] HELP_TAGS = new String[]{"describe", "desc", "extended", "table"};

  static {
    try {
      if (reservedPrintFields == null) {
        Properties properties = PluginUtil.getPluginProperty(DescribeTableCommand.class);
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

  public static void printUsage(PrintStream stream, ExecutionContext ctx) {
    stream.println("Usage:");
    if (ctx.isProjectMode()) {
      stream.println("  describe|desc [extended] [<project name>.]<table name>"
                     + " [partition(<partition spec>)];");
      stream.println("Examples:");
      stream.println("  desc my_table;");
      stream.println("  desc my_table partition(foo='bar');");
      stream.println("  desc extended my_table;");
      stream.println("  desc my_project.my_table;");
      // stream.println("  desc my_project.my_schema.my_table;");
    } else {
      stream.println("  describe|desc [extended] [[<project name>.]<schema name>.]<table name>"
                     + " [partition(<partition spec>)];");
      stream.println("Examples:");
      stream.println("  desc my_table;");
      stream.println("  desc my_table partition(foo='bar');");
      stream.println("  desc extended my_table;");
      stream.println("  desc my_schema.my_table;");
      stream.println("  desc my_project.my_schema.my_table;");
    }
  }

  public DescribeTableCommand(
      String cmd,
      ExecutionContext cxt,
      Coordinate coordinate,
      boolean isExtended
      ) {
    super(cmd, cxt);
    this.coordinate = coordinate;
    this.isExtended = isExtended;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    coordinate.interpretByCtx(getContext());
    String project = coordinate.getProjectName();
    String schema = coordinate.getSchemaName();
    String table = coordinate.getObjectName();
    String partition = coordinate.getPartitionSpec();

    if (table == null || table.length() == 0) {
      throw new OdpsException(
          ErrorCode.INVALID_COMMAND
          + ": Invalid syntax - DESCRIBE|DESC [EXTENDED] table_name [PARTITION(partition_col = 'partition_col_value';");
    }

    DefaultOutputWriter writer = this.getContext().getOutputWriter();

    Odps odps = getCurrentOdps();

    Table t = odps.tables().get(project, schema, table);

    writer.writeResult(""); // for HiveUT

    String result;
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

  private static final String EXTENDED_GROUP = "extended";
  private static Pattern PATTERN = Pattern.compile(
      "\\s*(DESCRIBE|DESC)\\s+(?<extended>EXTENDED\\s+)?" + Coordinate.TABLE_PATTERN,
      Pattern.CASE_INSENSITIVE|Pattern.DOTALL);

  public static DescribeTableCommand parse(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {
    Matcher m = PATTERN.matcher(cmd);
    if (!m.matches()) {
      return null;
    }

    boolean extended = m.group(EXTENDED_GROUP) != null;
    Coordinate coordinate = Coordinate.getTableCoordinate(m, cxt);

    return new DescribeTableCommand(cmd, cxt, coordinate, extended);
  }

  private String getScreenDisplay(Table t, Partition meta) throws ODPSConsoleException {
    return getBasicScreenDisplay(t, meta) + getExtendedScreenDisplay(t, meta);
  }

  private String getSpace(int len) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      sb.append(' ');
    }
    return sb.toString();
  }

  // port from task/sql_task/query_result_helper.cpp:PrintTableMeta
  private String getBasicScreenDisplay(Table t, Partition meta) throws ODPSConsoleException {
    StringWriter out = new StringWriter();
    PrintWriter w = new PrintWriter(out);

    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    try {

      if (meta != null) { // partition meta
        w.println("+------------------------------------------------------------------------------------+");
        w.printf("| PartitionSize: %-67d |\n", meta.getSize());
        w.println("+------------------------------------------------------------------------------------+");
        w.printf("| CreateTime:               %-56s |\n", df.format(meta.getCreatedTime()));
        w.printf("| LastDDLTime:              %-56s |\n", df.format(meta.getLastMetaModifiedTime()));
        w.printf("| LastModifiedTime:         %-56s |\n", df.format(meta.getLastDataModifiedTime()));
        if (meta.getLastDataAccessTime() != null) {
          w.printf("| LastAccessTime:           %-56s |\n", df.format(meta.getLastDataAccessTime()));
        }
        w.println("+------------------------------------------------------------------------------------+");
      } else { // table meta

        w.println("+------------------------------------------------------------------------------------+");
        w.printf("| Owner:                    %-56s |\n", t.getOwner());
        w.printf("| Project:                  %-56s |\n", t.getProject());
        if (getContext().isSchemaMode()) {
          w.printf("| Schema:                   %-56s |\n", t.getSchemaName());
        }
        w.printf("| TableComment: %-68s |\n", t.getComment());
        w.println("+------------------------------------------------------------------------------------+");
        w.printf("| CreateTime:               %-56s |\n", df.format(t.getCreatedTime()));
        w.printf("| LastDDLTime:              %-56s |\n", df.format(t.getLastMetaModifiedTime()));
        w.printf("| LastModifiedTime:         %-56s |\n", df.format(t.getLastDataModifiedTime()));
        if (t.getLastDataAccessTime() != null) {
          w.printf("| LastAccessTime:           %-56s |\n", df.format(t.getLastDataAccessTime()));
        }
        // Lifecyle
        if (t.getLife() != -1) {
          w.printf("| Lifecycle:                %-56d |\n", t.getLife());
        }
        // HubLifecyle
        if (t.getHubLifecycle() != -1) {
          w.printf("| HubLifecycle:             %-56d |\n", t.getHubLifecycle());
        }
        w.println("+------------------------------------------------------------------------------------+");
        // Label
        if (!StringUtils.isNullOrEmpty(t.getMaxLabel())) {
          w.printf("| TableLabel:               %-56s |\n", t.getTableLabel());
          w.printf("| MaxLabel:                 %-56s |\n", t.getMaxLabel());
          w.println("+------------------------------------------------------------------------------------+");
        }

        if (isExtended && !StringUtils.isNullOrEmpty(t.getMaxExtendedLabel())) {
          w.printf("| TableExtendedLabel:       %-56s |\n",
                   CollectionUtils.isEmpty(t.getTableExtendedLabels()) ? " " : StringUtils
                       .join(t.getTableExtendedLabels().toArray(), ","));
          w.printf("| MaxExtendedLabel:         %-56s |\n", t.getMaxExtendedLabel());
          w.println(
              "+------------------------------------------------------------------------------------+");
        }

        if (t.isExternalTable()) {
          w.println("| ExternalTable: YES                                                                 |");
        } else if (t.isVirtualView()) {
          w.println("| VirtualView  : YES                                                                 |");
          w.printf("| ViewText: %-72s |\n", t.getViewText());
        } else if (t.isMaterializedView()) {
          w.println("| MaterializedView: YES                                                              |");
          w.printf("| ViewText: %-72s |\n", t.getViewText());
          w.printf("| Rewrite Enabled: %-65s |\n", t.isMaterializedViewRewriteEnabled());
          w.printf("| AutoRefresh Enabled: %-61s |\n", t.isAutoRefreshEnabled());

          if (t.isAutoSubstituteEnabled() != null) {
            w.printf("| AutoSubstitute Enabled: %-58s |\n", t.isAutoSubstituteEnabled());
          }
          if (t.getRefreshInterval() != null) {
            w.printf("| Refresh Interval Minutes: %-56s |\n", t.getRefreshInterval());
          }
          if (t.getRefreshCron() != null) {
            w.printf("| Refresh Cron: %-68s |\n", t.getRefreshCron());
          }
        } else {
          w.printf("| InternalTable: YES      | Size: %-50d |\n", t.getSize());
        }

        w.println("+------------------------------------------------------------------------------------+");
        w.println("| Native Columns:                                                                    |");
        w.println("+------------------------------------------------------------------------------------+");
        String columnFormat =
            isExtended ? "| %-8s | %-6s | %-5s | %-13s | %-8s | %-12s | %-12s |\n"
                       : "| %-15s | %-10s | %-5s | %-43s |\n";
        String columnHeader =
            isExtended ? String
                .format(columnFormat, "Field", "Type", "Label", "ExtendedLabel", "Nullable", "DefaultValue", "Comment")
                       : String.format(columnFormat, "Field", "Type", "Label", "Comment");

        w.printf(columnHeader);
        w.println(
            "+------------------------------------------------------------------------------------+");

        for (Column c : t.getSchema().getColumns()) {
          String labelOutput = "";
          if (c.getCategoryLabel() != null) {
            labelOutput = c.getCategoryLabel();
          }

          if (isExtended) {
            String extendedLabels = "";
            if (!CollectionUtils.isEmpty(c.getExtendedlabels())) {
              extendedLabels = StringUtils.join(c.getExtendedlabels().toArray(), ",");
            }

            String defaultValueStr = "NULL";
            if (c.hasDefaultValue()) {
              defaultValueStr = c.getDefaultValue();
            }

            w.printf(columnFormat, c.getName(),
                     c.getTypeInfo().getTypeName().toLowerCase(), labelOutput, extendedLabels,
                     c.isNullable(), defaultValueStr,
                     c.getComment());
          } else {
            w.printf(columnFormat, c.getName(),
                     c.getTypeInfo().getTypeName().toLowerCase(), labelOutput, c.getComment());
          }

        }
        w.println("+------------------------------------------------------------------------------------+");

        if (t.getSchema().getPartitionColumns().size() > 0) {
          w.println("| Partition Columns:                                                                 |");
          w.println("+------------------------------------------------------------------------------------+");

          for (Column c : t.getSchema().getPartitionColumns()) {
            w.printf("| %-15s | %-10s | %-51s |\n", c.getName(),
                     c.getTypeInfo().getTypeName().toLowerCase(), c.getComment());
          }

          w.println("+------------------------------------------------------------------------------------+");
        }
      } // end else
    } catch (Exception e) {
      throw new ODPSConsoleException(ErrorCode.INVALID_RESPONSE + ": Invalid table schema.", e);
    }
    w.flush();
    w.close();

    return out.toString();
  }

  private String getExtendedScreenDisplay(Table t, Partition pt) throws ODPSConsoleException {
    if (!isExtended || t.isVirtualView()) {
      return "";
    }

    StringWriter out = new StringWriter();
    PrintWriter w = new PrintWriter(out);
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    try {
      if (pt != null) {
        if (pt.getLifeCycle() != -1) {
          w.printf("| LifeCycle:                %-56s |\n", pt.getLifeCycle());
        }
        w.printf("| IsExstore:                %-56s |\n", pt.isExstore());
        w.printf("| IsArchived:               %-56s |\n", pt.isArchived());
        w.printf("| PhysicalSize:             %-56s |\n", pt.getPhysicalSize());
        w.printf("| FileNum:                  %-56s |\n", pt.getFileNum());

        if (!CollectionUtils.isEmpty(reservedPrintFields) && !StringUtils
            .isNullOrEmpty(pt.getReserved())) {
          JsonObject object = new JsonParser().parse(pt.getReserved()).getAsJsonObject();
          for (String key : reservedPrintFields) {
            if (object.has(key)) {
              int spaceLength = Math.max((25 - key.length()), 1);
              w.printf(String.format("| %s:%-" + spaceLength + "s%-56s |\n", key, " ",
                                     object.get(key).getAsString()));
            }
          }
        }

        boolean isAcid2Table = t.isTransactional() && t.getPrimaryKey() != null && !t.getPrimaryKey().isEmpty();
        if (pt.getClusterInfo() != null && !isAcid2Table) {
          appendClusterInfo(pt.getClusterInfo(), w);
        }
        if (isAcid2Table) {
          appendAcidInfo(t, pt.getClusterInfo(), w);
        }
        // 具体分区 显示分层存储信息
        if (pt.getStorageTierInfo() != null) {
          if (pt.getStorageTierInfo().getStorageTier() != null) {
            w.printf("| StorageTier:              %-56s |\n",
                     pt.getStorageTierInfo().getStorageTier().getName());
          }
          Date lastModifiedTime = pt.getStorageTierInfo().getStorageLastModifiedTime();
          if (lastModifiedTime != null) {
            w.printf("| StorageTierLastModifiedTime:  %-52s |\n", df.format(lastModifiedTime));
          }
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
              w.printf(
                  "| %-20s: %-60s |\n",
                  entry.getKey(),
                  StringEscapeUtils.escapeJava(entry.getValue()));
            }
          }
        } else if (!t.isVirtualView()) {
          if (t.isMaterializedView()) {
            w.printf("| IsOutdated:               %-56s |\n", t.isMaterializedViewOutdated());
          }
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

        boolean isAcid2Table = t.isTransactional() && t.getPrimaryKey() != null && !t.getPrimaryKey().isEmpty();
        if (t.getClusterInfo() != null && !isAcid2Table) {
          appendClusterInfo(t.getClusterInfo(), w);
        }
        if (isAcid2Table) {
          appendAcidInfo(t, t.getClusterInfo(), w);
        }
        // storageTier 需要区分是分区表还是非分区表
        if (t.isPartitioned()) { //分区表显示汇总
          StorageTierInfo storageTierInfo = t.getStorageTierInfo();
          if (storageTierInfo != null) {
            for (StorageTier tier : StorageTier.values()) {
              if (storageTierInfo.getStorageSize(tier) != null) {
                w.printf("| %s:%s %-56d |\n", tier.getSizeName(),
                         getSpace(24 - tier.getSizeName().length()),
                         storageTierInfo.getStorageSize(tier));
              }
            }
          }
        } else { //非分区表只显示类型和修改时间
          if (t.getStorageTierInfo() != null) {
            if (t.getStorageTierInfo().getStorageTier() != null) {
              w.printf("| StorageTier:              %-56s |\n",
                       t.getStorageTierInfo().getStorageTier().getName());
            }
            Date lastModifiedTime = t.getStorageTierInfo().getStorageLastModifiedTime();
            if (lastModifiedTime != null) {
              w.printf("| StorageTierLastModifiedTime:  %-52s |\n", df.format(lastModifiedTime));
            }
          }
        }

        w.println(
            "+------------------------------------------------------------------------------------+");
      }

      if (t.isMaterializedView()) {
        List<Map<String, String>> history = t.getRefreshHistory();
        if (history != null && history.size() != 0) {
          w.println(
              "| AutoRefresh History:                                                               |");
          w.println(
              "+------------------------------------------------------------------------------------+");

          String columnFormat = "| %-25s | %-10s | %-19s | %-19s |";
          String
              columnHeader =
              String.format(columnFormat, "InstanceId", "Status", "StartTime", "EndTime");

          w.println(columnHeader);
          w.println(
              "+------------------------------------------------------------------------------------+");

          for (Map<String, String> map : history) {
            String id = map.get("InstanceId");
            String status = map.get("Status");
            String startTime = CommandUtils.longToDateTime(map.get("StartTime"));
            String endTime = CommandUtils.longToDateTime(map.get("EndTime"));
            String res = String.format(columnFormat, id, status, startTime, endTime);
            w.println(res);
          }
          w.println(
              "+------------------------------------------------------------------------------------+");
        }
      }
    } catch (Exception e) {
      throw new ODPSConsoleException(ErrorCode.INVALID_RESPONSE + ": Invalid table schema.", e);
    }

    w.flush();
    w.close();

    return out.toString();
  }

  private void appendAcidInfo(Table table, Table.ClusterInfo clusterInfo, PrintWriter writer) {
    if (table.getPrimaryKey() != null && !table.getPrimaryKey().isEmpty()) {
      writer.printf("| Primarykey:               %-56s |\n", Arrays.toString(table.getPrimaryKey().toArray()));
    }
    if (table.getAcidDataRetainHours() >= 0) {
      writer.printf("| acid.data.retain.hours:   %-56s |\n", table.getAcidDataRetainHours());
    }
    if (clusterInfo.getBucketNum() != -1) {
      writer.printf("| write.bucket.num:         %-56s |\n", clusterInfo.getBucketNum());
    }
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
}
