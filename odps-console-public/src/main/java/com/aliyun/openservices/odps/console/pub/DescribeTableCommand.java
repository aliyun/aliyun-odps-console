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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;

import com.aliyun.odps.Column;
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
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils.TablePart;

/**
 * Describe table meta
 * 
 * DESCRIBE|DESC table_name [PARTITION(partition_col = 'partition_col_value',
 * ...)];
 */
public class DescribeTableCommand extends AbstractCommand {

  private String project;
  private String table;
  private String partition;

  public static final String[] HELP_TAGS = new String[]{"describe", "desc", "table"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: describe|desc [<projectname>.]<tablename> [partition(<spec>)]");
  }

  public DescribeTableCommand(String cmd, ExecutionContext cxt, String project, String table,
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
              + ": Invalid syntax - DESCRIBE|DESC table_name [PARTITION(partition_col = 'partition_col_value';");
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

  private static Pattern PATTERN = Pattern.compile("\\s*(DESCRIBE|DESC)\\s+(.*)",
      Pattern.CASE_INSENSITIVE|Pattern.DOTALL);

  public static DescribeTableCommand parse(String cmd, ExecutionContext cxt) {
    if (cmd == null || cxt == null) {
      return null;
    }

    DescribeTableCommand r = null;
    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (!match) {
      return r;
    }

    cmd = m.group(2);

    TablePart tablePart = ODPSConsoleUtils.getTablePart(cmd);

    if (tablePart.tableName != null) {
      String[] tableSpec = ODPSConsoleUtils.parseTableSpec(tablePart.tableName);
      String project = tableSpec[0];
      String table = tableSpec[1];

      r = new DescribeTableCommand(cmd, cxt, project, table, tablePart.partitionSpec);
    }

    return r;
  }

  private String getScreenDisplay(Table t, Partition meta) throws ODPSConsoleException {
    return getScreenDisplay(t, meta, false);
  }

  // port from task/sql_task/query_result_helper.cpp:PrintTableMeta
  public static String getScreenDisplay(Table t, Partition meta, boolean isExtended) throws ODPSConsoleException {
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
        w.println("+------------------------------------------------------------------------------------+");
      } else { // table meta

        w.println("+------------------------------------------------------------------------------------+");
        w.printf("| Owner: %-20s | Project: %-43s |\n", t.getOwner(), t.getProject());
        w.printf("| TableComment: %-68s |\n", t.getComment());
        w.println("+------------------------------------------------------------------------------------+");
        w.printf("| CreateTime:               %-56s |\n", df.format(t.getCreatedTime()));
        w.printf("| LastDDLTime:              %-56s |\n", df.format(t.getLastMetaModifiedTime()));
        w.printf("| LastModifiedTime:         %-56s |\n", df.format(t.getLastDataModifiedTime()));
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
        }
        else if (!t.isVirtualView()) {
          w.printf("| InternalTable: YES      | Size: %-50d |\n", t.getSize());
        } else {
          w.printf("| VirtualView  : YES  | ViewText: %-50s |\n", t.getViewText());
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
}
