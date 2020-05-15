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

import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.CommandParserUtils;
import com.aliyun.openservices.odps.console.utils.SessionUtils;

/**
 * ExecuteCommand只为-e参数准备，也只包含其它的执行命令
 *
 * @author shuman.gansm
 **/
public class ExecuteCommand extends AbstractCommand {

  public ExecuteCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    if (getContext().getAutoSessionMode()) {
      SessionUtils.autoAttachSession(getContext(), getCurrentOdps());
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

  /**
   * 通过传递的参数，解析出对应的command
   **/
  public static AbstractCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    // 处理query的执行
    // parse -e参数，这个parse是可以被交互模式互用的
    String option = "-e";

    if (optionList.contains(option)) {
      if (optionList.indexOf(option) + 1 < optionList.size()) {

        int index = optionList.indexOf(option);
        // 创建相应的command列表
        String arg = optionList.get(index + 1);

        // 消费掉-e , -f 及参数
        optionList.remove(optionList.indexOf(option));
        optionList.remove(optionList.indexOf(arg));

        if (!arg.endsWith(";")) {
          arg = arg + ";";
        }
        return new ExecuteCommand(arg, sessionContext);
      }
    }
    return null;
  }
}
