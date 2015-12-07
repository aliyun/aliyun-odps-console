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
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

/**
 * @author shuman.gansm
 * */
public class SkipCommand extends AbstractCommand {

  public SkipCommand(int newStep, String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  public void run() throws OdpsException, ODPSConsoleException {

    // getContext().setStep(newStep);
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static SkipCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    if (optionList.contains("-k")) {
      if (optionList.indexOf("-k") + 1 < optionList.size()) {

        int index = optionList.indexOf("-k");
        // 一定要这样,不能 通过index来remove
        String stepStr = optionList.get(index + 1);

        int step = 0;
        ;
        try {
          step = Integer.valueOf(stepStr);
        } catch (NumberFormatException e) {
          throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND + ":need int value["
              + stepStr + "]");
        }

        optionList.remove(optionList.indexOf("-k"));
        optionList.remove(stepStr);

        // skip只是设置一个context的内容，而且skip需要在parser阶段设置skip的值，因为命令需要在命令的统一入口skip掉command
        sessionContext.setStep(step);

      }

    }

    return null;
  }
}
