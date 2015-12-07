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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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

import jline.console.UserInterruptException;

public class ShowInstanceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"show", "list", "ls", "instance",
                                                        "processlist", "process", "instances"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: show p [<number>]");
    stream.println("       show proc|processlist");
    stream.println("       show|ls|list instances [<number>]");
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

  // 同步多个线程
  private CountDownLatch ct;

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
    for (; insListing.hasNext(); ) {
      ODPSConsoleUtils.checkThreadInterrupted();

      Instance instance = insListing.next();
      instanceList.add(instance);
    }

    // 找到所有instance，如果有效率问题,需要从后面开始找

    List<String[]> showList = new LinkedList<String[]>();

    Collections.sort(instanceList, new InstanceComparator());
    String instanceTitle[] = {"StartTime", "RunTime", "Status", "InstanceID", "Owner", "Query"};
    showList.add(instanceTitle);

    int fromIndex = 0;
    int endIndex = 0;

    // 如果number数量过多，需要用多线程来处理
    for (int i = 0; i < number; i++) {
      ODPSConsoleUtils.checkThreadInterrupted();


      if (i >= instanceList.size()) {

        break;
      }
      Instance instanceInfo = instanceList.get(i);

      String instanceAttr[] = new String[6];
      instanceAttr[4] = instanceInfo.getOwner();
      instanceAttr[3] = instanceInfo.getId();
      instanceAttr[2] = "";
      instanceAttr[0] = ODPSConsoleUtils.formatDate(instanceInfo.getStartTime());

      if (instanceInfo.getEndTime() != null && instanceInfo.getStartTime() != null) {
        instanceAttr[1] = (instanceInfo.getEndTime().getTime() - instanceInfo.getStartTime()
            .getTime()) / 1000 + "s";
      } else if (instanceInfo.getEndTime() == null && instanceInfo.getStartTime() != null) {
        // 当instance为running时，可能为空
        instanceAttr[1] = (new Date().getTime() - instanceInfo.getStartTime().getTime()) / 1000
                          + "s";
      } else {
        instanceAttr[1] = "";
      }

      // 插入到第一个位置，可以再排序
      showList.add(1, instanceAttr);

    }

    ct = new CountDownLatch(showList.size() - 1);

    // 通过多个线程读取instance source
    // 从1开始,第一行为title
    for (int i = 1; i < showList.size(); ) {

      i = i + 20;
      fromIndex = i - 20;
      if (i >= showList.size()) {
        endIndex = showList.size();
      } else {
        endIndex = i;
      }

      // 起一个新的线程
      FetchScourceThread t = new FetchScourceThread(showList, fromIndex, endIndex, project, odps);
      Thread td = new Thread(t);
      td.start();

    }

    try {
      ct.await();
    } catch (InterruptedException e) {
      throw new UserInterruptException("In ShowInstanceCommand:run(), InterruptedException occur.");
    }

    // 设置每一列的百分比
    int columnPercent[] = {15, 6, 6, 20, 22, 53};
    ODPSConsoleUtils.formaterTable(showList, columnPercent, getContext().getConsoleWidth());

    getWriter().writeError((showList.size() - 1) + " instances");

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

  public static ShowInstanceCommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    String upCommandText = commandString.toUpperCase().trim();
    if (upCommandText.toUpperCase().matches("\\s*SHOW\\s+P\\s*")
        || upCommandText.toUpperCase().matches("\\s*SHOW\\s+PROCESSLIST\\s*")
        || upCommandText.toUpperCase().matches("\\s*SHOW\\s+PROC\\s*")
        || upCommandText.toUpperCase().matches("\\s*SHOW\\s+P\\s+[\\s\\S]*")

        // bug #262924
        || upCommandText.toUpperCase().matches("\\s*SHOW\\s+INSTANCES\\s*")
        || upCommandText.toUpperCase().matches("\\s*SHOW\\s+INSTANCES\\s+[\\s\\S]*")
        || upCommandText.toUpperCase().matches("\\s*(LS|LIST)\\s+INSTANCES\\s*")
        || upCommandText.toUpperCase().matches("\\s*(LS|LIST)\\s+INSTANCES\\s+[\\s\\S]*")) {
      List<String> paraList = new ArrayList<String>(Arrays.asList(upCommandText.trim()
                                                                      .split("\\s+")));

      // 删除前面两个命令选项
      String firstCmd = paraList.remove(0);
      paraList.remove(0);

      int length = paraList.size();
      if (length == 0) {

        // 不带参数的情况，默认取50条记录，从当天开始,到现在的记录
        return new ShowInstanceCommand(commandString, sessionContext, null, getTime(new Date(), 0),
                                       null,
                                       50);
      }

      // ls instances -p project_name
      // ls instances --limit 100
      String project = null;

      // 默认50
      Integer number = 50;

      if (firstCmd.equals("LS") || firstCmd.equals("LIST")) {
        AntlrObject antlr = new AntlrObject(commandString);
        List<String> commandList = Arrays.asList(antlr.getTokenStringArray());

        CommandLine cl = getCommandLine(commandList.toArray(new String[length]));
        if (2 < cl.getArgs().length) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
                                         + "[invalid parameters]");
        }

        project = cl.getOptionValue("p");

        if (cl.hasOption("limit")) {
          try {
            number = Integer.parseInt(cl.getOptionValue("limit"));
            if (number < 1) {
              throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
                                             + "[number should >=1]");
            }
          } catch (NumberFormatException e) {
            // 命令错误，不是整型数
            throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
                                           + "[number is not integer]");
          }

        } else if (project == null) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[invalid paras]");
        }

        return new ShowInstanceCommand(commandString, sessionContext, project,
                                       getTime(new Date(), 0),
                                       null,
                                       number);

      }

      SimpleDateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");

      Date fromDate = null;
      Date toDate = null;
      try {
        // 把from和to参数从命令中取出来
        int fromIndex = paraList.indexOf("FROM");
        if (fromIndex >= 0 && fromIndex + 1 < paraList.size()) {

          String fromString = paraList.get(fromIndex + 1);
          fromDate = formatDate.parse(fromString);

          paraList.remove(fromIndex + 1);
          paraList.remove(fromIndex);
        }

        int toIndex = paraList.indexOf("TO");
        if (toIndex >= 0 && toIndex + 1 < paraList.size()) {
          String toString = paraList.get(toIndex + 1);
          toDate = formatDate.parse(toString);

          paraList.remove(toIndex + 1);
          paraList.remove(toIndex);
        }
      } catch (ParseException e1) {
        // 命令错误，不是整型数
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[invalid date]");
      }

      // 最后一个就是linenumber
      if (paraList.size() == 1) {

        try {
          number = Integer.valueOf(paraList.get(0));
        } catch (NumberFormatException e) {
          // 命令错误，不是整型数
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND
                                         + "[number is not integer]");
        }

        paraList.remove(0);
      }

      if (paraList.size() == 0) {

        // 如果没日期，默认为当天开始
        if (fromDate == null && toDate == null) {
          fromDate = new Date();
        }

        return new ShowInstanceCommand(commandString, sessionContext, project, getTime(fromDate, 0),
                                       getTime(toDate, 24), number);
      } else {
        // 命令错误，不是整型数
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + "[more parameter]");
      }
    }
    return null;
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

  class FetchScourceThread implements Runnable {

    String project;
    List<String[]> insList;
    int formIndex;
    int toIndex;
    Odps odps;

    public FetchScourceThread(List<String[]> insList, int formIndex, int toIndex, String project, Odps odps) {
      super();
      this.insList = insList;
      this.formIndex = formIndex;
      this.toIndex = toIndex;
      this.project = project;
      this.odps = odps;
    }

    @Override
    public void run() {

      for (int i = formIndex; i < toIndex; i++) {

        String instanceAttr[] = insList.get(i);

        Instance instance = odps.instances().get(project, instanceAttr[3]);
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
        ct.countDown();
      }

    }
  }
}
