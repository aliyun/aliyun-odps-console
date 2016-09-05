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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.InstanceFilter;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

public class ShowInstanceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "list", "ls", "instance",
                                                        "processlist", "proc", "instances"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: show p|proc|processlist [from <yyyy-MM-dd>] [to <yyyy-MM-dd>] [-p <project>] [-limit <number> | <number>]");
    stream.println("       ls|list instances [from <yyyy-MM-dd>] [to <yyyy-MM-dd>] [-p <project>] [-limit <number> | <number>]");
  }

  private Date fromDate;
  private Date toDate;

  private Integer number;

  private String project;

  public Date getFromDate() {
    return fromDate;
  }

  public Date getToDate() {
    return toDate;
  }

  public Integer getNumber() {
    return number;
  }

  static Options initOptions() {
    Options opts = new Options();
    Option project_name = new Option("p", true, "project name");
    Option limit_number = new Option("limit", true, "show limit");

    project_name.setRequired(false);
    limit_number.setRequired(false);

    opts.addOption(project_name);
    opts.addOption(limit_number);

    return opts;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    Odps odps = getCurrentOdps();

    InstanceFilter filter = new InstanceFilter();
    filter.setFromTime(fromDate);
    filter.setEndTime(toDate);
    filter.setOnlyOwner(true);
    Iterator<Instance> insListing;

    if (project == null) {
      project = odps.getDefaultProject();
    }

    insListing = odps.instances().iterator(project, filter);

    ArrayList<Instance> instanceList = new ArrayList<Instance>();
    // 获取到所有的 instance
    for (; insListing.hasNext(); ) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Instance instance = insListing.next();
      instanceList.add(instance);
    }

    // 按照时间先后倒排序所有的 instance
    Collections.sort(instanceList, new InstanceComparator());
    String [] instanceTitle = {"StartTime", "RunTime", "Status", "InstanceID", "Owner", "Query"};
    // 设置每一列的百分比
    int [] columnPercent = {15, 6, 6, 20, 22, 53};

    ODPSConsoleUtils.formaterTableRow(instanceTitle, columnPercent, getContext().getConsoleWidth());
    // instanceList 是倒排序的，limit 的时候取集合头部数据。
    // 但是显示的时候要将顺序调整，按时间先后顺序显示。
    int count = number < instanceList.size() ? number : instanceList.size();
    int index = count;
    while(index > 0) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Instance instance = instanceList.get(--index);

      printInstanceInfo(instance, columnPercent);
    }

    getWriter().writeError(count + " instances");
  }

  private void printInstanceInfo(Instance instance, int [] columnPercent) {
    String instanceAttr[] = new String[6];

    instanceAttr[0] = ODPSConsoleUtils.formatDate(instance.getStartTime());

    if (instance.getEndTime() != null && instance.getStartTime() != null) {
      instanceAttr[1] = (instance.getEndTime().getTime() - instance.getStartTime()
          .getTime()) / 1000 + "s";
    } else if (instance.getEndTime() == null && instance.getStartTime() != null) {
      // 当instance为running时，可能为空
      instanceAttr[1] = (new Date().getTime() - instance.getStartTime().getTime()) / 1000
                        + "s";
    } else {
      instanceAttr[1] = "";
    }
    instanceAttr[5] = "";
    instanceAttr[4] = instance.getOwner();
    instanceAttr[3] = instance.getId();
    instanceAttr[2] = "";

    // get instance for more details
    try {
      List<Task> tasks = instance.getTasks();
      instanceAttr[5] = "";
      for (Task task : tasks) {
        instanceAttr[5] += task.getCommandText();

        TaskStatus status = instance.getTaskStatus().get(task.getName());
        if (status == null) {
          instanceAttr[2] = "Waiting";
        } else {
          instanceAttr[2] = StringUtils.capitalize(instance.getTaskStatus().get(task.getName())
                                                       .getStatus().toString().toLowerCase());
        }
      }
    } catch (OdpsException e) {
    }

    ODPSConsoleUtils.formaterTableRow(instanceAttr, columnPercent, getContext().getConsoleWidth());
  }

  public ShowInstanceCommand(String commandText, ExecutionContext context, String project,
                             Date fromDate,
                             Date toDate, Integer number) {
    super(commandText, context);

    this.project = project;
    this.fromDate = fromDate;
    this.toDate = toDate;
    this.number = number;

  }

  static CommandLine getCommandLine(String[] args) throws ODPSConsoleException {
    if (args == null || args.length < 2) {
      throw new ODPSConsoleException("Invalid parameters - Generic options must be specified.");
    }

    Options opts = initOptions();
    CommandLineParser clp = new GnuParser();
    CommandLine cl;
    try {
      cl = clp.parse(opts, args, false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }

    return cl;
  }

  private static final Pattern LIST_PATTERN = Pattern.compile(
      "\\s*(LS|LIST)\\s+INSTANCES($|\\s+([\\s\\S]*))", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final Pattern SHOW_PATTERN = Pattern.compile(
      "\\s*SHOW\\s+(P|PROCESSLIST|PROC|INSTANCES)($|\\s+([\\s\\S]*))", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static Date getDateParam(List<String> tokens, String key) throws ODPSConsoleException {
    SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");
    Date date = null;
    int index = tokens.indexOf(key);
    if (index >= 0) {
      if (tokens.size() > (index + 1)) {
        try {
          String value = tokens.get(index + 1);
          date = formatDate.parse(value);

          // consume tokens
          tokens.remove(index + 1);
          tokens.remove(index);
        } catch (ParseException e) {
          // 命令错误，不是整型数
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[invalid date]");
        }
      } else {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[missing " + key + " parameter]");
      }
    }

    return date;
  }

  private static ShowInstanceCommand parseInternal(String commandString,
                                                   String extCommandString,
                                                   ExecutionContext sessionContext)
      throws ODPSConsoleException {
    int number = 50;
    String project = null;
    if ((extCommandString == null) || extCommandString.trim().isEmpty()) {
      // 不带参数的情况，默认取50条记录，从当天开始,到现在的记录
      return new ShowInstanceCommand(commandString, sessionContext, project, getTime(new Date(), 0),
                                     null, number);
    }

    extCommandString = extCommandString.toLowerCase();
    List<String> tokens = new ArrayList<String>(Arrays.asList(new AntlrObject(extCommandString).getTokenStringArray()));

    // 把from和to参数从命令中取出来
    Date fromDate = getDateParam(tokens, "from");
    Date toDate = getDateParam(tokens, "to");

    // 如果没日期，默认为当天开始
    if (fromDate == null && toDate == null) {
      fromDate = new Date();
    }

    if (tokens.size() == 1) {
      // number
      number = getNumberToken(tokens.get(0));
    } else if (tokens.size() > 1) {
      // number, -p, -limit
      CommandLine cl = getCommandLine(tokens.toArray(new String[0]));

      // 除了 -p 和 -limit 选项，最多还剩下一个参数 <number>, 且这个参数与 －limit 不共存
      if (cl.getArgs().length > 1 || (cl.getArgs().length == 1 && cl.hasOption("limit"))) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[more parameter]");
      }

      // 获取 project
      if (cl.hasOption("p")) {
        project = cl.getOptionValue("p");
      }

      // 获取 limit or number
      String numberStr = null;
      if (cl.hasOption("limit")) {
        numberStr = cl.getOptionValue("limit");
      } else if (cl.getArgs().length == 1) {
        numberStr = cl.getArgs()[0];
      }

      if (numberStr != null) {
        number = getNumberToken(numberStr);
      }
    }

    return new ShowInstanceCommand(commandString, sessionContext, project, getTime(fromDate, 0),
                                   getTime(toDate, 0), number);
  }

  private static int getNumberToken(String numberString) throws ODPSConsoleException {
    int number;
    try {
      number = Integer.parseInt(numberString);
      if (number < 1) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[number should >=1]");
      }
    } catch (NumberFormatException e) {
      // 命令错误，不是整型数
      throw new ODPSConsoleException(
          ODPSConsoleConstants.BAD_COMMAND + "[number is not integer]");
    }
    return number;
  }

  public static ShowInstanceCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    Matcher listMatcher = LIST_PATTERN.matcher(commandString);
    Matcher showMatcher = SHOW_PATTERN.matcher(commandString);
    String extCommandString = null;

    if (listMatcher.matches()){
      extCommandString = listMatcher.group(listMatcher.groupCount());
    } else if (showMatcher.matches()) {
      extCommandString = showMatcher.group(listMatcher.groupCount());
    } else {
      return null;
    }

    return parseInternal(commandString, extCommandString, sessionContext);
  }

  private class InstanceComparator implements Comparator<Instance> {

    @Override
    public int compare(Instance o1, Instance o2) {
      String start1 = ODPSConsoleUtils.formatDate(o1.getStartTime());
      String start2 = ODPSConsoleUtils.formatDate(o2.getStartTime());

      return start2.compareTo(start1);
    }

  }

  private static Date getTime(Date date, int hour) {

    if (date == null) {
      return null;
    }
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    cal.set(Calendar.HOUR_OF_DAY, hour);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }
}
