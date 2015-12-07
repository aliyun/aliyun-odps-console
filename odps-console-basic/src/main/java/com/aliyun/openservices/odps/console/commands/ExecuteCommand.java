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
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import com.aliyun.openservices.odps.console.utils.QueryUtil;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

/**
 * ExecuteCommand只为-e参数准备，也只包含其它的执行命令
 * 
 * @author shuman.gansm
 * **/
public class ExecuteCommand extends AbstractCommand {

  public static final int DEFAULT_BATCH_NUMBER = 999;
  private String option;

  // 执行的sql数，默认999，ots返回1000
  private int batchNumber = DEFAULT_BATCH_NUMBER;

  public String getOption() {
    return option;
  }

  public int getBatchNumber() {
    return batchNumber;
  }
  public ExecuteCommand(String commandText, ExecutionContext context, String option) {
    this(commandText, context, option, DEFAULT_BATCH_NUMBER);
  }
  public ExecuteCommand(String commandText, ExecutionContext context, String option, int batchNumber) {
    super(commandText, context);
    this.option = option;
    this.batchNumber = batchNumber;

  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    if (getContext().isHtmlMode()) {
      AbstractCommand command = CommandParserUtils.parseCommand(getCommandText(), getContext());
      try {
        Document document = Jsoup.parse(new File("html/index.html"), "utf-8");
        String st = command.runHtml(document);
        System.out.println(st);
      } catch (IOException e) {
        throw new OdpsException(e.getMessage(), e);
      }
      return;
    }

    if (option.equals("-fb")) {

      List<String> sqlList = new AntlrObject(getCommandText()).splitCommands();
      // sql 文件的批处理
      QueryUtil.batchExecuteSql(getCurrentOdps(), sqlList, getBatchNumber(), getContext());

    } else {
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

  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static AbstractCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    // 处理query的执行
    // parse -e参数，这个parse是可以被交互模式互用的
    if (optionList.contains("-e")) {

      if (optionList.indexOf("-e") + 1 < optionList.size()) {

        int index = optionList.indexOf("-e");
        // 创建相应的command列表
        String cmd = optionList.get(index + 1);

        // 消费掉-e 及参数
        optionList.remove(optionList.indexOf("-e"));
        optionList.remove(optionList.indexOf(cmd));

        if (!cmd.endsWith(";")) {
          cmd = cmd + ";";
        }

        return new ExecuteCommand(cmd, sessionContext, "-e");
      }

    } else if (optionList.contains("-f")) {

      if (optionList.indexOf("-f") + 1 < optionList.size()) {

        String option = "-f";
        int batchNumber = DEFAULT_BATCH_NUMBER;
        if (optionList.contains("-b") && optionList.indexOf("-b") + 1 < optionList.size()) {

          try {
            batchNumber = Integer.valueOf(optionList.get(optionList.indexOf("-b") + 1));
            optionList.remove(optionList.get(optionList.indexOf("-b") + 1));
          } catch (NumberFormatException e) {
            throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + ":-b need int value["
                + batchNumber + "]");
          }

          optionList.remove(optionList.indexOf("-b"));
          option = "-fb";
        }

        int index = optionList.indexOf("-f");
        // 创建相应的command列表
        String filename = optionList.get(index + 1);

        // 消费掉-e 及参数
        optionList.remove(optionList.indexOf("-f"));
        optionList.remove(optionList.indexOf(filename));

        if (filename.trim().endsWith(";")) {
          filename = filename.substring(0, filename.lastIndexOf(";"));
        }
        String fileContent = FileUtil.getStringFromFile(filename);

        // 当文件内容为空时,不再继续执行
        if (fileContent.trim().equals("")) {
          return null;
        }

        // 如果文件最后是注释，可能没有分号
        if (!fileContent.endsWith(";")) {
          fileContent = fileContent + ";";
        }

        return new ExecuteCommand(fileContent, sessionContext, option,
            batchNumber);
      }
    }

    return null;
  }

}
