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
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.openservices.odps.console.ErrorCode;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils.TablePart;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;

/**
 * List partitions
 * <p/>
 * SHOW PARTITIONS [project_name.]<table_name>;
 *
 * @author <a
 *         href="shenggong.wang@alibaba-inc.com">shenggong.wang@alibaba-inc.com
 *         </a>
 */
public class ShowPartitionsCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "list", "ls", "partition", "partitions"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: show partitions [<projectname>.]<tablename> [partition(<spec>)]");
    stream.println("       list|ls partitions [-p,-project <projectname>] <tablename> [(<spec>)]");
  }

  private String project;
  private String table;
  private String partition;

  public ShowPartitionsCommand(String cmd, ExecutionContext cxt, String project, String table,
                               String partition) {
    super(cmd, cxt);

    this.project = project;
    this.table = table;
    this.partition = partition;
  }

  static Options initOptions() {
    Options opts = new Options();
    Option project_name = new Option("p", true, "project name");
    project_name.setRequired(false);

    opts.addOption(project_name);

    return opts;
  }

  static CommandLine getCommandLine(String[] commandText) throws ODPSConsoleException {
    Options opts = initOptions();
    CommandLineParser clp = new GnuParser();
    CommandLine cl;
    try {
      cl = clp.parse(opts, commandText, false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }

    return cl;
  }

  /*
   * (non-Javadoc)
   *
   * @see com.aliyun.openservices.odps.console.commands.AbstractCommand#run()
   */
  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    if (table == null || table.length() == 0) {
      throw new OdpsException(ErrorCode.INVALID_COMMAND
                              + ": Invalid syntax - SHOW PARTITIONS [project.]<table>[ partition(partitionSpec)];");
    }

    DefaultOutputWriter writer = getContext().getOutputWriter();

    Odps odps = OdpsConnectionFactory.createOdps(getContext());

    if (null == project) {
      project = getCurrentProject();
    }

    Table t = odps.tables().get(project, table);
    Iterator<Partition> parts = null;
    if (partition != null) {
      parts = t.getPartitionIterator(new PartitionSpec(partition));
    } else {
      parts = t.getPartitionIterator();
    }
    writer.writeResult(""); // for HiveUT
    for (; parts.hasNext(); ) {
      ODPSConsoleUtils.checkThreadInterrupted();

      String p = parts.next().getPartitionSpec().toString();
      p = p.replaceAll("\'", ""); // 兼容旧版本不带引号的输出格式
      p = p.replaceAll(",", "/"); // 兼容SQLTask输出的格式-。-
      writer.writeResult(p);
    }

    writer.writeError("\nOK");
  }

  private static final Pattern PATTERN = Pattern
      .compile("\\s*SHOW\\s+PARTITIONS\\s+(.*)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
  private static final Pattern PUBLIC_PATTERN =
      Pattern.compile("\\s*(LS|LIST)\\s+PARTITIONS\\s+(.*)",
                      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  public static ShowPartitionsCommand parse(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {
    if (cmd == null || cxt == null) {
      return null;
    }

    ShowPartitionsCommand r = null;

    Matcher m = PATTERN.matcher(cmd);
    Matcher pubMatcher = PUBLIC_PATTERN.matcher(cmd);

    boolean match = m.matches();
    boolean pubMatch = pubMatcher.matches();

    TablePart tablePart = null;

    if (!match && !pubMatch) {
      return r;
    }

    if (pubMatch) {
      tablePart = getTablePartFromPublicCommand(pubMatcher.group(2));
    } else {
      tablePart = ODPSConsoleUtils.getTablePart(m.group(1));
    }

    if (tablePart == null || tablePart.tableName == null) {
      throw new ODPSConsoleException(ErrorCode.INVALID_COMMAND
                                     + ": Invalid syntax - SHOW PARTITIONS [project.]<table>[ partition(partitionSpec)];");
    }

    String[] tableSpec = ODPSConsoleUtils.parseTableSpec(tablePart.tableName);
    String project = tableSpec[0];
    String table = tableSpec[1];

    r = new ShowPartitionsCommand(cmd, cxt, project, table, tablePart.partitionSpec);

    return r;
  }

  public static TablePart getTablePartFromPublicCommand(String target) throws ODPSConsoleException {

    String project = null;
    TablePart tablePart = null;

    String originalTarget = target;

    AntlrObject antlr = new AntlrObject(target);
    String[] args = antlr.getTokenStringArray();

    if (args == null || args.length < 1) {
      return null;
    }

    CommandLine cl = getCommandLine(args);

    if (cl.getArgList().size() < 1) {
      return null;
    }

    if (cl.hasOption("p")) {
      project = cl.getOptionValue("p");
      target = StringUtils.join(args, "", 2, args.length);
    } else {
      target = originalTarget;
    }

    tablePart = ODPSConsoleUtils.getTablePartFromPubCommand(target);
    if (null != project) {
      String table = tablePart.tableName;
      tablePart.tableName = project + "." + table;
    }

    return tablePart;
  }

}
