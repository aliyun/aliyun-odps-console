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

import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import java.util.List;

public class LoginCommand extends AbstractCommand {
  private String accessId;
  private String accessKey;

  // 帐号认证信息
  private String accessToken;
  private String accountProvider;

  public String getAccessId() {
    return accessId;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getAccessToken() {
    return accessToken;
  }

  public String getAccountProvider() {
    return accountProvider;
  }

  public LoginCommand(String accountProvider, String accessId, String accessKey, String accessToken,
      String commandText, ExecutionContext context) {
    super(commandText, context);
    this.accessId = accessId;
    this.accessKey = accessKey;
    this.accessToken = accessToken;
    this.accountProvider = accountProvider;
  }

  public LoginCommand(String accessId, String accessKey, String commandText,
      ExecutionContext context) {
    super(commandText, context);
    this.accessId = accessId;
    this.accessKey = accessKey;
  }

  // check the userName && password
  public void run() throws OdpsException, ODPSConsoleException {

    if (accessId != null) {
      getContext().setAccessId(accessId);
    }
    if (accessKey != null) {
      getContext().setAccessKey(accessKey);
    }
    if (accountProvider != null) {
      getContext().setAccountProvider(accountProvider);
    }
    if (accessToken != null) {
      getContext().setAccessToken(accessToken);
    }
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static LoginCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    LoginCommand command = null;

    // 处理login, 这是默认的方式，走aliyun的签名认证
    if (optionList.contains("-u") && optionList.contains("-p")
        && optionList.indexOf("-u") + 1 < optionList.size()
        && optionList.indexOf("-p") + 1 < optionList.size()) {

      String uPara = optionList.get(optionList.indexOf("-u") + 1);
      String pPara = optionList.get(optionList.indexOf("-p") + 1);

      // 如果-u\-p后面还是命令，则是无效的命令
      if (uPara.startsWith("-") || pPara.startsWith("-")) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }

      command = new LoginCommand(uPara, pPara, "-u-p", sessionContext);

      // 消费掉这一个参数
      optionList.remove(optionList.indexOf("-u"));
      optionList.remove(optionList.indexOf("-p"));
      optionList.remove(optionList.indexOf(uPara));
      optionList.remove(optionList.indexOf(pPara));

    } else if (optionList.contains("--account-provider") && optionList.contains("--access-id")
        && optionList.contains("--access-key")) {

      // 添加account-provider和access-id\access-key登录的组合
      String accountProvider = ODPSConsoleUtils.shiftOption(optionList, "--account-provider");
      String clientId = ODPSConsoleUtils.shiftOption(optionList, "--access-id");
      String clientSecret = ODPSConsoleUtils.shiftOption(optionList, "--access-key");
      command = new LoginCommand(accountProvider, clientId, clientSecret, null,
                                 "--account-provider", sessionContext);
    }

    return command;
  }

}
