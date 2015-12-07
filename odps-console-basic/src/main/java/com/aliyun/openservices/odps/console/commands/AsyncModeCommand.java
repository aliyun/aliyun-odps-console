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

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * @author shuman.gansm
 * */
public class AsyncModeCommand extends AbstractCommand {

  public AsyncModeCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public void run() throws OdpsException, ODPSConsoleException {

    // 设置异常执行模式
    getContext().setAsyncMode(true);
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static AsyncModeCommand parse(List<String> optionList, ExecutionContext sessionContext) {

    if (optionList.contains("-N")) {

      optionList.remove(optionList.indexOf("-N"));

      return new AsyncModeCommand("-N", sessionContext);

    }

    return null;
  }
}
