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

package com.aliyun.openservices.odps.console.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;

import jline.console.UserInterruptException;

/**
 * odps console util类
 *
 * @author shuman.gansm
 */
public class ODPSConsoleUtils {

  public static String formatDate(Date date) {
    if (date == null) {
      return "";
    }

    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault()).format(date);
  }

  /**
   * 采用空格来补齐
   *
   * @param contentList
   *     : 二维数组的表格内容
   * @param columnPercent
   *     : 表格各列的百分比，columnPercent中数值和为100
   * @param width
   *     : 控制台的总宽度
   */
  @SuppressWarnings("resource")
  public static void formaterTable(List<String[]> contentList, int[] columnPercent, int width) {

    // columnPercent.lenth必须和String[].length的一致，且，columnPercent中数值和为100
    Formatter f1 = new Formatter(System.out);
    for (String[] str : contentList) {
      checkThreadInterrupted();

      formaterTableRow(str, columnPercent, width);
    }
    f1.flush();
  }

  public static void formaterTableRow(String[] str, int[] columnPercent, int width) {
    Formatter f1 = new Formatter(System.out);
    for (int i = 0; i < columnPercent.length; i++) {
      StringBuilder formatter = new StringBuilder();
      int diswidth = (int) (((double) columnPercent[i] / 100) * width);

      if (str[i] == null || str[i].length() < diswidth) {
        // 小于就对齐，大于就不管了
        formatter.append("%1$-").append(diswidth).append("s");
        f1.format(formatter.toString(), str[i]);
      } else {
        f1.format("%s ", str[i]);
      }
    }
    f1.format("\n");
  }

  /**
   * 取当前操作系统的宽度，默认返回150
   */
  public static int getConsoleWidth() {

    // 默认150
    int result = 150;
    return result;
  }

  public static boolean isWindows() {
    boolean flag = false;
    if (System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS") != -1) {
      flag = true;
    }
    return flag;
  }

  /**
   * 取conf文件夹的路径
   */
  public static String getConfigFilePath() {
    URL url =  ODPSConsoleUtils.class.getClassLoader().getResource("odps_config.ini");
    if (url == null) {
      return null;
    } else {
      return url.getFile();
    }
  }

  private static String cltVersion = "";
  private static String osName = "";
  private static String userIp = "";
  private static String userHostname = "";
  private static String mvnVersion;

  public static String getMvnVersion() {
    if (StringUtils.isNullOrEmpty(mvnVersion)) {
      getUserAgent();
    }

    return mvnVersion;
  }

  public static String getUserAgent() {

    if (StringUtils.isNullOrEmpty(cltVersion)) {

      InputStream is = null;
      try {
        is = ODPSConsoleUtils.class
            .getResourceAsStream("/com/aliyun/openservices/odps/console/version.txt");
        Properties properties = new Properties();
        properties.load(is);
        mvnVersion = properties.getProperty("MavenVersion");
        cltVersion = String.format("CLT(%s : %s)", mvnVersion,
                                   properties.getProperty("Revision"));
      } catch (Exception e) {
      } finally {
        try {
          is.close();
        } catch (IOException e) {
        }
      }
    }
    if (StringUtils.isNullOrEmpty(osName)) {
      osName = System.getProperties().getProperty("os.name");
    }
    if (StringUtils.isNullOrEmpty(userIp)) {
      try {
        userIp = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
      }
    }
    if (StringUtils.isNullOrEmpty(userHostname)) {
      try {
        userHostname = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
      }
    }

    String systemInfo = osName + "(" + userIp + "/" + userHostname + ")";

    return cltVersion + "; " + systemInfo;
  }

  public static String shiftOption(List<String> optionList, String option) {
    if (optionList.contains(option) && optionList.indexOf(option) + 1 < optionList.size()) {

      int index = optionList.indexOf(option);
      // 创建相应的command列表
      String cmd = optionList.get(index + 1);

      // 消费掉-e 及参数
      optionList.remove(optionList.indexOf(option));
      optionList.remove(optionList.indexOf(cmd));

      return cmd;
    }

    return null;
  }

  public static class TablePart {

    public String tableName;
    public String partitionSpec;
  }

  private static final Pattern PATTERN = Pattern.compile(
      "\\s*([\\w\\.]+)(\\s*|(\\s+PARTITION\\s*\\((.*)\\)))\\s*", Pattern.CASE_INSENSITIVE);

  public static TablePart getTablePart(String cmd) {
    TablePart r = new TablePart();

    Matcher m = PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (match && m.groupCount() >= 1) {
      r.tableName = m.group(1);
    }

    if (match && m.groupCount() >= 4) {
      r.partitionSpec = m.group(4);
    }

    return r;
  }

  private static final Pattern PUB_PATTERN = Pattern.compile(
      "\\s*([\\w\\.]+)(\\s*|(\\s*\\((.*)\\)))\\s*", Pattern.CASE_INSENSITIVE);

  public static TablePart getTablePartFromPubCommand(String cmd) {
    TablePart r = new TablePart();

    Matcher m = PUB_PATTERN.matcher(cmd);
    boolean match = m.matches();

    if (match && m.groupCount() >= 1) {
      r.tableName = m.group(1);
    }

    if (match && m.groupCount() >= 3) {
      r.partitionSpec = m.group(4);
    }

    return r;
  }

  /**
   * @param str
   *     like project.table or table
   * @return the first item is project name, the second one is table name
   */
  public static String[] parseTableSpec(String str) {
    String[] r = new String[2];

    if (str != null && str.length() > 0) {
      int idx = str.indexOf('.');
      if (idx == -1) {
        r[1] = str;
      } else {
        r[0] = str.substring(0, idx);
        r[1] = str.substring(idx + 1, str.length());
      }
    }

    if (r[0] != null && r[0].trim().length() == 0) {
      r[0] = null;
    }
    if (r[1] != null && r[1].trim().length() == 0) {
      r[1] = null;
    }

    return r;
  }

  public static String generateLogView(Odps odps, Instance instance, ExecutionContext context) {
    try {
      String logview = odps.logview().generateLogView(instance, 7 * 24);
      return logview;
    } catch (Exception e) {
      context.getOutputWriter().writeError("Generate LogView Failed:" + e.getMessage());
    }
    return null;
  }

  /**
   * Crack a command line.
   *
   * @param line
   *     the command line to process.
   * @return the command line broken into strings. An empty or null toProcess parameter results in a
   * zero sized array.
   */
  public static String[] translateCommandline(String line) throws ODPSConsoleException {
    if (StringUtils.isNullOrEmpty(line)) {
      return new String[0];
    }
    // parse with a simple finite state machine

    final int normal = 0;
    final int inQuote = 1;
    final int inDoubleQuote = 2;
    int state = normal;
    StringTokenizer tok = new StringTokenizer(line, "\"\' \t\n\r\f", true);
    ArrayList<String> v = new ArrayList<String>();
    StringBuffer current = new StringBuffer();
    boolean lastTokenHasBeenQuoted = false;

    while (tok.hasMoreTokens()) {
      String nextTok = tok.nextToken();
      switch (state) {
        case inQuote:
          if ("\'".equals(nextTok)) {
            lastTokenHasBeenQuoted = true;
            state = normal;
            current.append(nextTok);
          } else {
            current.append(nextTok);
          }
          break;
        case inDoubleQuote:
          if ("\"".equals(nextTok)) {
            lastTokenHasBeenQuoted = true;
            state = normal;
            current.append(nextTok);
          } else {
            current.append(nextTok);
          }
          break;
        default:
          if ("\'".equals(nextTok)) {
            state = inQuote;
            current.append(nextTok);
          } else if ("\"".equals(nextTok)) {
            state = inDoubleQuote;
            current.append(nextTok);
          } else if (nextTok.matches("\\s")) {
            if (lastTokenHasBeenQuoted || current.length() != 0) {
              v.add(current.toString());
              current = new StringBuffer();
            }
          } else {
            current.append(nextTok);
          }
          lastTokenHasBeenQuoted = false;
          break;
      }
    }
    if (lastTokenHasBeenQuoted || current.length() != 0) {
      v.add(current.toString());
    }
    if (state == inQuote || state == inDoubleQuote) {
      throw new ODPSConsoleException("unbalanced quotes in " + line);
    }
    return v.toArray(new String[]{});
  }

  public synchronized static String runCommand(AbstractCommand command) throws OdpsException, ODPSConsoleException {
    ByteArrayOutputStream content = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(content);
    PrintStream oldout = System.out;
    PrintStream olderr = System.err;
    System.setOut(stream);
    System.setErr(stream);
    command.run();
    System.setOut(oldout);
    System.setErr(olderr);
    return content.toString();
  }

  public static void checkThreadInterrupted() {
    if (Thread.currentThread().interrupted()) {
      //do sth
      throw new UserInterruptException("thread interrupted");
    }
    return;
  }
}
