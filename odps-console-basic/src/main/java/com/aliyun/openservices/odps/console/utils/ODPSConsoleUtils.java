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
import java.lang.Character.UnicodeBlock;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.reflect.MethodUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.LazyLoad;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;

import org.jline.reader.UserInterruptException;

/**
 * odps console util类
 *
 * @author shuman.gansm
 */
public class ODPSConsoleUtils {
  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault());

  public static String formatDate(Date date) {
    if (date == null) {
      return "";
    }

    return DATE_FORMAT.format(date);
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
        Properties properties = new ExtProperties();
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
        String hostname = InetAddress.getLocalHost().getHostName();
        if (!containsHanBlock(hostname)) {
          userHostname = hostname;
        }
      } catch (UnknownHostException e) {
      }
    }

    String systemInfo = osName + "(" + userIp + "/" + userHostname + ")";

    return cltVersion + "; " + systemInfo;
  }

  public static boolean containsHanBlock(String s) {
    for (int i = 0; i < s.length(); ) {
      int codepoint = s.codePointAt(i);
      UnicodeBlock block = Character.UnicodeBlock.of(codepoint);
      i += Character.charCount(codepoint);
      if (block == UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS ||
          block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A ||
          block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B ||
          block == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS ||
          block == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS_SUPPLEMENT) {
        return true;
      }
    }
    return false;
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
      return odps.logview().generateLogView(instance, context.getLogViewLife());
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
    if (Thread.interrupted()) {
      //do sth
      throw new UserInterruptException("thread interrupted");
    }
    return;
  }

  private static ODPSConsoleReader odpsConsoleReader = null;

  public static ODPSConsoleReader getOdpsConsoleReader() throws ODPSConsoleException {
    if (odpsConsoleReader == null) {
      odpsConsoleReader = new ODPSConsoleReader();
    }
    return odpsConsoleReader;
  }

  public static String safeGetString(LazyLoad resource, String methodName) {
    Object res = safeGetObject(resource, methodName);
    return res == null ? " " : res.toString();
  }

  public static String safeGetDateString(LazyLoad resource, String methodName) {
    Object res = safeGetObject(resource, methodName);
    return res == null ? " " : ODPSConsoleUtils.formatDate((Date) res);
  }

  public static Object safeGetObject(LazyLoad resource, String methodName) {
    try {
      return MethodUtils.invokeMethod(resource, methodName, null);
    } catch (Exception ex) {
      return null;
    }
  }

  /**
   * 对比两个版本字符串
   *
   * @param left
   *    版本号1
   * @param right
   *    版本号2
   * @return
   *    若 left = right, 返回 0; 若 left < right 返回 －1; 若 left > right 返回 1
   */

  public static int compareVersion(String left, String right) {
    if (left.equals(right)) {
      return 0;
    }

    String pattern = "[\\\\.\\\\_\\\\-]";
    String[] leftArray = left.split(pattern);
    String[] rightArray = right.split(pattern);

    int length = leftArray.length < rightArray.length ? leftArray.length : rightArray.length;

    for (int i = 0; i < length; i++) {
      if (rightArray[i].equalsIgnoreCase(leftArray[i])) {
        continue;
      }

      if (org.apache.commons.lang.StringUtils.isNumeric(rightArray[i])
          && org.apache.commons.lang.StringUtils.isNumeric(leftArray[i])) {
        if (Integer.parseInt(rightArray[i]) > Integer.parseInt(leftArray[i])) {
          return -1;
        } else if (Integer.parseInt(rightArray[i]) < Integer.parseInt(leftArray[i])) {
          return 1;
        }
      } else {
        int res = leftArray[i].compareToIgnoreCase(rightArray[i]);

        return  res > 0 ? 1 : res;
      }
      // 相等 比较下一组值
    }

    if (leftArray.length == rightArray.length) {
      return 0;
    } else {
      return leftArray.length < rightArray.length ? -1 : 1;
    }
  }

  public static Map<String, Integer> getDisplayWidth(List<Column> columns,
                                                     List<Column> partitionsColumns,
                                                     List<String> selectColumns) {
    if (columns == null || columns.isEmpty()) {
      return null;
    }

    Map<String, Integer> displayWith = new LinkedHashMap<String, Integer>();
    Map<String, OdpsType> fieldTypeMap = new LinkedHashMap<String, OdpsType>();
    // Get Column Info from Table Meta
    for (Column column : columns) {
      fieldTypeMap.put(column.getName(), column.getTypeInfo().getOdpsType());
    }

    // Get Partition Info from Table Meta
    if (partitionsColumns != null) {
      for (Column column : partitionsColumns) {
        fieldTypeMap.put(column.getName(), column.getTypeInfo().getOdpsType());
      }
    }

    // delete the column which is not in columns
    if (selectColumns != null && !selectColumns.isEmpty()) {
      Set<String> set = fieldTypeMap.keySet();
      List<String> remainList = new ArrayList<String>(set);
      Iterator<String> it = set.iterator();
      while (it.hasNext()) {
        Object o = it.next();
        if (selectColumns.contains(o))
          remainList.remove(o);
      }

      for (Object o : remainList) {
        fieldTypeMap.remove(o);
      }
    }
    // According the fieldType to calculate the display width
    for (Map.Entry entry : fieldTypeMap.entrySet()) {
      if ("BOOLEAN".equalsIgnoreCase(entry.getValue().toString()))
        displayWith.put(entry.getKey().toString(), entry.getKey().toString().length() > 4 ? entry
            .getKey().toString().length() : 4);
      else
        displayWith.put(entry.getKey().toString(), entry.getKey().toString().length() > 10 ? entry
            .getKey().toString().length() : 10);
    }

    return displayWith;
  }

  public static String makeOutputFrame(Map<String, Integer> displayWidth) {
    StringBuilder sb = new StringBuilder();
    sb.append("+");
    for (int width : displayWidth.values()) {
      for (int i = 0; i < width + 2; i++)
        sb.append("-");
      sb.append("+");
    }
    return sb.toString();
  }

  public static String makeTitle(List<Column> columns, Map<String, Integer> displayWidth) {
    StringBuilder titleBuf = new StringBuilder();
    titleBuf.append("| ");
    for (Column column : columns) {
      String str = column.getName();
      titleBuf.append(str);
      if (str.length() < displayWidth.get(str)) {
        for (int j = 0; j < displayWidth.get(str) - str.length(); j++) {
          titleBuf.append(" ");
        }
      }
      titleBuf.append(" | ");
    }
    return titleBuf.toString();
  }
}
