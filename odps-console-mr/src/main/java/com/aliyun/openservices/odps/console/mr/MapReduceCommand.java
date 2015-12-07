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

package com.aliyun.openservices.odps.console.mr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.mapreduce.runtime.MapReduceJob;
import com.aliyun.openservices.odps.console.mapreduce.runtime.MapReduceJobLauncher;

public class MapReduceCommand extends AbstractCommand {

  public static final String[] HELP_TAGS = new String[]{"mapreduce", "mr", "jar", "openmr"};

  public static void printUsage(PrintStream out) {
    out.println("");
    out.println("Usage: jar [<genericOptions>] <mainClass> args...;");
    out.println("");
    out.println("Generic options supported are");
    out.println("    -conf <configuration file>         application configuration file");
    out.println("    -resources <resource_name_list>    file/archive/table resources used in mapper or reducer");
    out.println("    -libjars <rsource_name_list>       jar resources used in mapper or reducer");
    out.println("    -classpath <local_file_list>       classpaths used to run mainClass");
    out.println("    -l                                 run job in local mode");
    out.println("    -D<prop_name>=<prop_value>         property value pair, which will be used to run mainClass");
    out.println("For example:");
    out.println("    jar -conf /home/admin/myconf -resources a.txt -libjars example.jar -classpath ../lib/example.jar:./other_lib.jar -Djava.library.path=./native -Xmx512M mycompany.WordCount -m 10 -r 10 in out;");
    out.println("");
  }

  private final static String OPT_CONF = "-conf";
  private final static String OPT_RESOURCES = "-resources";
  private final static String OPT_LIBJARS = "-libjars";
  private final static String OPT_CLASSPATH = "-classpath";
  private final static String OPT_CP = "-cp";
  private final static String OPT_L = "-l";
  private final static String OPT_D = "-D";
  private final static String OPT_X = "-X";
  private final static String TEMP_RESOURCE_PREFIX = "file:";

  private String conf;
  private String resources;
  private String libjars;
  private String classpath;
  private boolean localMode;
  private Map<String, List<String>> tempResources = new HashMap<String, List<String>>();
  private List<String> jvmOptions = new ArrayList<String>();
  private String remainderArgs;

  public MapReduceCommand(String commandText, ExecutionContext context) throws ODPSConsoleException {
    super(commandText, context);
    parseGeneralOptions(commandText);
  }

  public String getConf() {
    return conf;
  }

  public String getResources() {
    return resources;
  }

  public String getLibjars() {
    return libjars;
  }

  public List<String> getJvmOptions() {
    return jvmOptions;
  }

  public String getClasspath() {
    return classpath;
  }

  public String getArgs() {
    return remainderArgs;
  }

  public boolean isLocalMode() {
    return localMode;
  }

  public Map<String, List<String>> getTempResources() {
    return tempResources;
  }

  private void parseGeneralOptions(String commandText) throws ODPSConsoleException {
    try {
      internalParseGeneralOptions(commandText);
      if (StringUtils.isBlank(remainderArgs)) {
        System.err.println("Syntax error: mainClass must be specified");
        printUsage(System.err);
        throw new IllegalArgumentException("mainClass not specified.");
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      throw new ODPSConsoleException("Parse general options error: " + e.getMessage(), e);
    }
  }

  private void internalParseGeneralOptions(String commandText) throws ODPSConsoleException,
      IOException {
    // split command by whitespace chars (space, tab, newline)
    String[] ss = StringUtils.splitPreserveAllTokens(commandText.trim());
    int idx = 1; // skip 'jar' command
    while (idx < ss.length) {
      // skip empty tokens;
      while (idx < ss.length && ss[idx].isEmpty()) {
        idx++;
      }
      if (idx >= ss.length) {
        break;
      }

      String token = ss[idx];

      if (token.equalsIgnoreCase(OPT_CONF)) {
        // skip empty tokens
        do {
          idx++;
        } while (idx < ss.length && ss[idx].isEmpty());

        if (idx < ss.length && isNotOpt(ss[idx])) {
          this.conf = ss[idx];
        } else {
          throw new IOException("Argument for conf can't be empty");
        }
        validateFiles(this.conf);

      } else if (token.equalsIgnoreCase(OPT_RESOURCES)) {
        // skip empty tokens
        do {
          idx++;
        } while (idx < ss.length && ss[idx].isEmpty());

        if (idx < ss.length && isNotOpt(ss[idx])) {
          this.resources = formatSeparator(ss[idx], false);
        } else {
          throw new IOException("Argument for resources can't be empty");
        }

      } else if (token.equalsIgnoreCase(OPT_LIBJARS)) {
        // skip empty tokens
        do {
          idx++;
        } while (idx < ss.length && ss[idx].isEmpty());

        if (idx < ss.length && isNotOpt(ss[idx])) {
          this.libjars = formatSeparator(ss[idx], true);
        } else {
          throw new IOException("Argument for libjars can't be empty");
        }

      } else if (token.equalsIgnoreCase(OPT_CLASSPATH) || token.equalsIgnoreCase(OPT_CP)) {
        // skip empty tokens
        do {
          idx++;
        } while (idx < ss.length && ss[idx].isEmpty());

        if (idx < ss.length && isNotOpt(ss[idx])) {
          this.classpath = formatSeparator(ss[idx], false);
        } else {
          throw new IOException("Argument for classpath can't be empty");
        }
        this.classpath = validateFiles(this.classpath);

      } else if (token.equals(OPT_L)) {
        localMode = true;
      } else if (token.startsWith(OPT_D)) {
        String[] kv = token.substring(OPT_D.length()).split("=", 2);
        if (kv.length == 2) {
          this.jvmOptions.add(token);
        } else {
          throw new IOException("Incorrect property: " + token);
        }

      } else if (token.startsWith(OPT_X)) {
        String xparam = token.substring(OPT_X.length());
        if (xparam.isEmpty()) {
          throw new IOException("Incorrect -X option, should not be empty");
        }
        this.jvmOptions.add(token);

      } else if (!token.isEmpty()) {
        // find main class
        // NOTE: tab and newline will be replaced with space
        StringBuilder builder = new StringBuilder();
        for (int i = idx; i < ss.length; i++) {
          if (i != idx) {
            builder.append(' ');
          }
          builder.append(ss[i]);
        }
        remainderArgs = builder.toString();

        break;

      }

      idx++;
    }
  }

  private boolean isNotOpt(String token) {
    return !token.startsWith("-");
  }

  private String validateFiles(String files) throws IOException {

    StringBuffer buf = new StringBuffer();
    String[] fileArr = files.split(",|" + System.getProperty("path.separator"));
    for (int i = 0; i < fileArr.length; i++) {
      String tmp = fileArr[i].trim();
      if (!new File(tmp).exists()) {
        throw new FileNotFoundException("File or Directory '" + tmp + "' does not exist.");
      }
      if (buf.length() > 0) {
        buf.append(System.getProperty("path.separator"));
      }
      buf.append(tmp);
    }
    return buf.toString();
  }

  private String formatSeparator(String itemlist, boolean isLibjars) throws IOException {
    if (itemlist == null)
      return null;
    StringBuffer buf = new StringBuffer();
    String[] items = itemlist.trim().isEmpty() ? new String[] {} : itemlist.trim().split(",");
    for (String item : items) {
      if (buf.length() > 0) {
        buf.append(',');
      }
      if (item.toLowerCase().startsWith(TEMP_RESOURCE_PREFIX)) {
        URL url = new URL(URLDecoder.decode(item, "utf-8"));
        File tempFile = new File(url.getPath());
        if (!tempFile.exists()) {
          throw new FileNotFoundException("File or Directory '" + item + "' does not exist.");
        }
        if (tempFile.isDirectory()) {
          throw new IOException("Temp resource not support directory '" + item + "'");
        }

        List<String> resInfo = new ArrayList<String>();
        resInfo.add(getResourceType(tempFile.getName(), isLibjars));
        resInfo.add(tempFile.getAbsolutePath());
        tempResources.put(tempFile.getName(), resInfo);
        buf.append(tempFile.getName());
      } else {
        buf.append(item);
      }
    }
    return buf.toString();
  }

  private String getResourceType(String name, boolean isLibjars) {
    String resNameSuffix = name.toUpperCase();
    if (resNameSuffix.endsWith(".PY")) {
      return "py";
    } else if (resNameSuffix.endsWith(".JAR")) {
      if (isLibjars) {
        return "jar";
      } else {
        return "archive";
      }
    } else if (resNameSuffix.endsWith(".ZIP") || resNameSuffix.endsWith(".TGZ")
        || resNameSuffix.endsWith(".TAR.GZ") || resNameSuffix.endsWith(".TAR")
        || resNameSuffix.endsWith(".ZIP")) {
      return "archive";
    }

    return "file";
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    String prjName = getContext().getProjectName();

    if (prjName == null || prjName.trim().equals("")) {
      throw new OdpsException(ODPSConsoleConstants.PROJECT_NOT_BE_SET);
    }

    MapReduceJobLauncher launcher = null;
    launcher = new MapReduceJob(this);

    launcher.run();
  }

  public static MapReduceCommand parse(String command, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    String trimCmd = command.trim();
    String jarCmd = trimCmd.replaceAll("\\s+", " ").toLowerCase();

    if (jarCmd.startsWith("jar ") || jarCmd.equals("jar")) {
      return new MapReduceCommand(trimCmd, sessionContext);
    }

    return null;
  }

}
