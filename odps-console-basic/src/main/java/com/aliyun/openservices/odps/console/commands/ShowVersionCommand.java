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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.FileUtil;

public class ShowVersionCommand extends AbstractCommand {

  public void run() throws OdpsException, ODPSConsoleException {

    InputStream is = null;
    try {
      is = this.getClass().getResourceAsStream("/com/aliyun/openservices/odps/console/version.txt");
      getWriter().writeResult(IOUtils.readStreamAsString(is));
    } catch (Exception e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.LOAD_VERSION_ERROR, e);
    } finally {
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
        }
      }
    }

  }

  public ShowVersionCommand(String commandText, ExecutionContext context) {
    super(commandText, context);
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static ShowVersionCommand parse(List<String> optionList, ExecutionContext sessionContext) {
    if (optionList.contains("-v") && optionList.size() == 1) {

      // 消费掉"-v"
      optionList.remove(optionList.indexOf("-v"));
      return new ShowVersionCommand("-v", sessionContext);
    } else if (optionList.contains("--version") && optionList.size() == 1) {
      optionList.remove(optionList.indexOf("--version"));
      return new ShowVersionCommand("--version", sessionContext);
    }

    return null;
  }
}
