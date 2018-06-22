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

import java.util.List;

import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.FileUtil;

/**
 * ExecuteFileCommand 只为 -f 参数准备
 *
 * @author zhenhong.gzh
 **/
public class ExecuteFileCommand extends ExecuteCommand {

  public ExecuteFileCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  /**
   * 通过传递的参数，解析出对应的command
   **/
  public static AbstractCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    String option = "-f";
    if (optionList.contains(option)) {
      if (optionList.indexOf(option) + 1 < optionList.size()) {

        int index = optionList.indexOf(option);
        // 创建相应的command列表
        String arg = optionList.get(index + 1);

        // 消费掉 -f 及参数
        optionList.remove(optionList.indexOf(option));
        optionList.remove(optionList.indexOf(arg));

        if (arg.trim().endsWith(";")) {
          arg = arg.substring(0, arg.lastIndexOf(";"));
        }
        String cmd = FileUtil.getStringFromFile(arg);

        // 当文件内容为空时,不再继续执行
        if (cmd.trim().equals("")) {
          return null;
        }

        if (!cmd.endsWith(";")) {
          cmd = cmd + ";";
        }
        return new ExecuteFileCommand(cmd, sessionContext);
      }
    }

    return null;
  }

}
