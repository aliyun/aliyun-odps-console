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

package com.aliyun.openservices.odps.console.commands;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.FileUtil;

/**
 * ExecuteCommand只为-e参数准备，也只包含其它的执行命令
 *
 * @author shuman.gansm
 **/
public class ExecuteCommand extends AbstractCommand {

  private String option;

  public String getOption() {
    return option;
  }

  public ExecuteCommand(String commandText, ExecutionContext context, String option) {
    super(commandText, context);
    this.option = option;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    if (getContext().isHtmlMode()) {
      AbstractCommand command = CommandParserUtils.parseCommand(getCommandText(), getContext());
      try {
        Document document = Jsoup.parse(new File("html/index.html"), "utf-8");
        command.runHtml(document);
        System.out.println(document);
      } catch (IOException e) {
        throw new OdpsException(e.getMessage(), e);
      }
      return;
    }

    AbstractCommand command;
    command = CommandParserUtils.parseCommand(getCommandText(), getContext());

    // 一条命令的情况,会直接走这里
    // 把命令都转成CompositeCommand来执行为了，为了兼容金融模式
    // ODPSConsoleUtils.printCommandStartMark(command);
    List<AbstractCommand> odpsCommandList = new LinkedList<AbstractCommand>();
    odpsCommandList.add(command);
    command = new CompositeCommand(odpsCommandList, "", getContext());

    command.run();
  }

  private static ExecuteCommand createCommandWithSentence(List<String> optionList,
                                                          ExecutionContext sessionContext,
                                                          String option)
      throws ODPSConsoleException {
    if (optionList.indexOf(option) + 1 < optionList.size()) {

      int index = optionList.indexOf(option);
      // 创建相应的command列表
      String arg = optionList.get(index + 1);

      // 消费掉-e , -f 及参数
      optionList.remove(optionList.indexOf(option));
      optionList.remove(optionList.indexOf(arg));

      String cmd = null;

      if (option.equals("-e")) {
        cmd = arg;
      } else if (option.equals("-f")) {
        if (arg.trim().endsWith(";")) {
          arg = arg.substring(0, arg.lastIndexOf(";"));
        }
        cmd = FileUtil.getStringFromFile(arg);

        // 当文件内容为空时,不再继续执行
        if (cmd.trim().equals("")) {
          return null;
        }
      }

      if (!cmd.endsWith(";")) {
        cmd = cmd + ";";
      }
      return new ExecuteCommand(cmd, sessionContext, option);
    }

    return null;
  }

  /**
   * 通过传递的参数，解析出对应的command
   **/
  public static AbstractCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    // 处理query的执行
    // parse -e参数，这个parse是可以被交互模式互用的
    if (optionList.contains("-e")) {
      return createCommandWithSentence(optionList, sessionContext, "-e");
    } else if (optionList.contains("-f")) {
      return createCommandWithSentence(optionList, sessionContext, "-f");
    }

    return null;
  }

}
