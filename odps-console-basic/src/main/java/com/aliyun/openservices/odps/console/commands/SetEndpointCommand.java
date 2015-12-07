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
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class SetEndpointCommand extends AbstractCommand {

  private String endPoint;

  public String getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(String endPoint) {
    this.endPoint = endPoint;
  }

  public SetEndpointCommand(String endPoint, String commandText, ExecutionContext context) {
    super(commandText, context);
    this.endPoint = endPoint;
  }

  public void run() throws OdpsException, ODPSConsoleException {
    getContext().setEndpoint(endPoint);
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static SetEndpointCommand parse(List<String> optionList, ExecutionContext sessionContext) {

    SetEndpointCommand command = null;

    if (optionList.contains("--endpoint")) {

      if (optionList.indexOf("--endpoint") + 1 < optionList.size()) {

        int index = optionList.indexOf("--endpoint");
        // 创建相应的command列表
        String cmd = optionList.get(index + 1);

        // 消费掉-e 及参数
        optionList.remove(optionList.indexOf("--endpoint"));
        optionList.remove(optionList.indexOf(cmd));

        return new SetEndpointCommand(cmd, "--endpoint=" + cmd, sessionContext);

      }
    }

    return command;
  }

}
