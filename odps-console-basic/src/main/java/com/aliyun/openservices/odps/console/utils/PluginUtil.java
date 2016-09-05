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

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

/**
 * 加载plugin的帮助类
 */
public class PluginUtil {

  /**
   * clt的根目录
   */
  public static String getRootPath() {

    // odpsconsole.jar的位置
    String path = ODPSConsoleUtils.class.getProtectionDomain().getCodeSource().getLocation()
        .getPath();
    if (path.endsWith(".jar")) {
      path = path.substring(0, path.lastIndexOf("/lib"));
    } else {
      // class路径，调试用
      if (path.lastIndexOf("/target") != -1) {
        path = path.substring(0, path.lastIndexOf("/target"));
      }
    }

    try {
      // 如果有空格的话，File(configFile)是找不到的
      path = URLDecoder.decode(path, "utf-8");
    } catch (Exception e1) {
    }

    return path;
  }

  /**
   * 取plugin的jar列表，加到classloader中
   */
  public static List<URL> getPluginsJarList() {

    List<URL> resultList = new ArrayList<URL>();

    String pluginsPath = getRootPath() + File.separator + "plugins";
    File pluginRoot = new File(pluginsPath);

    if (pluginRoot.exists()) {
      String[] plugins = pluginRoot.list();
      // 获取所有插件下lib目录的jar文件
      for (String pluginName : plugins) {

        File pluginLib = new File(pluginsPath + File.separator + pluginName + File.separator
                                  + "lib");
        // 只有目录才加载
        if (pluginLib.isDirectory()) {

          File[] jars = pluginLib.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
              if (file.isFile() && file.getName().toLowerCase().endsWith(".jar")) {
                return true;
              }
              return false;
            }
          });

          for (File jarFile : jars) {
            try {
              resultList.add(new URL("file:///" + jarFile.getAbsolutePath()));
            } catch (MalformedURLException e) {
              e.printStackTrace();
            }
          }
        }
      }
    }

    return resultList;
  }

  /**
   * PluginPriorityCommand 比较器
   * */
  public class ComparetorByPriority implements Comparator<PluginPriorityCommand> {

    public int compare(PluginPriorityCommand o1, PluginPriorityCommand o2) {
      if (o1.getCommandPriority() > o2.getCommandPriority()) {
        return 1;
      } else if (o1.getCommandPriority() == o2.getCommandPriority()) {
        return 0;
      }
      return -1;
    }
  }

  /**
   * 取扩展命令(带优先级)列表
   */

  public static List<PluginPriorityCommand> getExtendCommandList() {

    List<PluginPriorityCommand> commands = new ArrayList<PluginPriorityCommand>();

    String pluginsPath = getRootPath() + File.separator + "plugins";
    File pluginRoot = new File(pluginsPath);

    if (pluginRoot.exists()) {
      String[] plugins = pluginRoot.list();

      for (String pluginName : plugins) {
        File pluginIni = new File(pluginsPath + File.separator + pluginName + File.separator
                                  + "plugin.ini");

        if (pluginIni.exists() && pluginIni.isFile()) {

          FileInputStream pis = null;
          try {
            pis = new FileInputStream(pluginIni);
            Properties properties = new Properties();
            properties.load(pis);
            String c = properties.getProperty("command");
            getPriorityCommandFromString(commands, c);
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            IOUtils.closeQuietly(pis);
          }
        }
      }
    }

    Collections.sort(commands);

    return commands;
  }

  /**
   * 从一段特定格式的字符串中，获取到带权限值的命令，并添加到列表中
   *
   * @param commands
   *     命令列表
   * @param commandString
   *     一段特定格式的字符串,包括若干个命令名称和其对应的权限值。
   *     命令名称与权限值使用冒号(:)隔开，多个命令之间使用逗号(,)隔开。
   *     例如：test_command1:5,test_command2:6
   *     注：若没有指定优先级，默认为 0
   * @return
   */

  public static void getPriorityCommandFromString(List<PluginPriorityCommand> commands,
                                                  String commandString) throws Exception {
    try {
      if (commandString != null) {
        String[] parts = commandString.split(",");
        for (String cmd : parts) {
          if (cmd.split(":").length == 2) {
            commands.add(new PluginPriorityCommand(cmd.split(":")[0],
                                                   Float.parseFloat(cmd.split(":")[1])));
          } else {
            commands.add(new PluginPriorityCommand(cmd, 0));
          }
        }
      }
    } catch (NumberFormatException e) {
      throw new Exception("Priority number type error. Float is expected");
    }
  }

  public static Properties getPluginProperty(Class<?> cls) throws IOException {

    // odpsconsole.jar的位置
    String path = cls.getProtectionDomain().getCodeSource().getLocation().getPath();
    if (path.endsWith(".jar")) {
      path = path.substring(0, path.lastIndexOf("/lib"));
    } else {
      // class路径，调试用
      if (path.lastIndexOf("/target") != -1) {
        path = path.substring(0, path.lastIndexOf("/target"));
      }
    }

    path = path + "/plugin.ini";

    try {
      // 如果有空格的话，File(configFile)是找不到的
      path = URLDecoder.decode(path, "utf-8");
    } catch (Exception e1) {
    }

    FileInputStream configInputStream = null;
    configInputStream = new FileInputStream(path);
    Properties properties = new ExtProperties();
    try {
      properties.load(configInputStream);
    } finally {
      try {
        configInputStream.close();
      } catch (IOException e) {
      }
    }
    return properties;
  }

  public static  void printPluginCommandPriority() {
    List<PluginPriorityCommand> ecList = CommandParserUtils.getExtendedCommandList();

    StringWriter out = new StringWriter();
    PrintWriter w = new PrintWriter(out);

    w.printf(" %-50s |  %-50s \n", "Command", "Priority");
    w.println("+------------------------------------------------------------------------------------+");

    for (PluginPriorityCommand command : ecList) {
      String commandPath = command.getCommandName();
      String [] splits = commandPath.split("\\.");
      String commandName = splits[splits.length - 1];

      w.printf(" %-50s |  %-50s \n", commandName, command.getCommandPriority());
    }

    w.flush();
    w.close();

    System.out.println(out.toString());
  }
}
