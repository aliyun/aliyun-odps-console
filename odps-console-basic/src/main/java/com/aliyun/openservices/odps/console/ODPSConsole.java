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

package com.aliyun.openservices.odps.console;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;

/**
 * 程序的入口类
 * **/
public class ODPSConsole {
  public static void main(String[] args) throws ODPSConsoleException {
    String [] commandArgs = CommandParserUtils.getCommandArgs(args);

    List<String> options = new ArrayList<String>();
    // 取得用户设置的config文件，没有设置返回null
    String config = prepareOptions(commandArgs, options);

    // 创建session context
    ExecutionContext sessionContext = ExecutionContext.load(config);

    checkSDKEnviron();

    DefaultOutputWriter writer = sessionContext.getOutputWriter();

    writer.writeDebug("ODPSConsole Start");
    try {

      // 把apache.commons.logging关掉，不会打印出来显示给用户看
      System.setProperty("org.apache.commons.logging.Log",
          "org.apache.commons.logging.impl.NoOpLog");

      // sessionContext会传递到所有新创建的command
      // 解析命令行参数，由command来决定是交互模式还是非交互模式
      AbstractCommand oa = CommandParserUtils.parseOptions(
          config == null ? commandArgs : (String[]) options.toArray(new String[0]), sessionContext);

      oa.execute();
    } catch (OdpsException e) {
      writer.writeError(ODPSConsoleConstants.FAILED_MESSAGE + e.getMessage());
      if (StringUtils.isNullOrEmpty(e.getMessage())) {
        writer.writeError(StringUtils.stringifyException(e));
      }
      // 在debug模式，把出错信息的stack，输出出来
      writer.writeDebug(e);

      System.exit(1);
    } catch (ODPSConsoleException e) {
      writer.writeError(ODPSConsoleConstants.FAILED_MESSAGE + e.getMessage());
      if (StringUtils.isNullOrEmpty(e.getMessage())) {
        writer.writeError(StringUtils.stringifyException(e));
      }

      writer.writeDebug(e);

      System.exit(e.getExitCode());
    } catch (Exception e) {
      // 如果是未知的异常,
      e.printStackTrace();

      System.exit(1);
    }

    sessionContext.getOutputWriter().writeDebug("ODPSConsole End");
    // 正常退出
    System.exit(0);
  }

  private static void checkSDKEnviron() {
    String classpath = System.getProperty("java.class.path");
    String[] classpathEntries = classpath.split(File.pathSeparator);
    long count = 0L;
    for (String line : classpathEntries) {
      if (line.contains("odps-sdk-core") && (!line.contains("odps-sdk-core-internal"))) {
        ++ count;
      }
    }
    if (count > 1) {
      System.err.println("WARNING: detected sdk conflict in console lib folder. Please install console in a fresh new folder.");
    }
  }

  protected static String prepareOptions(String[] args, List<String> options) {

    String config = null;
    for (String option : args) {
      if (option.indexOf("--config") == 0) {
        String[] configOptions = option.split("=", 2);
        if (configOptions.length == 2) {
          config = configOptions[1];
        } else {
          System.err.println(ODPSConsoleConstants.FAILED_MESSAGE + "pls set config correctly.");
          System.exit(1);
        }
      } else {
        options.add(option);
      }
    }

    return config;
  }
}
