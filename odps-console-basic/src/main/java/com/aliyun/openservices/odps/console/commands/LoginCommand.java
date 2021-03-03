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
import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class LoginCommand extends AbstractCommand {

  private static final String OPTION_USER = "-u";
  private static final String OPTION_PASSWORD = "-p";
  private static final String LONG_OPTION_ACCOUNT_PROVIDER = "--account-provider";
  private static final String LONG_OPTION_ACCESS_ID = "--access-id";
  private static final String LONG_OPTION_ACCESS_KEY = "--access-key";
  private static final String LONG_OPTION_STS_TOKEN = "--sts-token";

  private AccountProvider accountProvider;
  private String accessId;
  private String accessKey;
  private String stsToken;

  public void setAccessId(String accessId) {
    this.accessId = accessId;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public void setStsToken(String stsToken) {
    this.stsToken = stsToken;
  }

  public LoginCommand(
      AccountProvider accountProvider,
      String commandText,
      ExecutionContext context) {
    super(commandText, context);
    this.accountProvider = accountProvider;
  }

  public LoginCommand(
      String accessId,
      String accessKey,
      String commandText,
      ExecutionContext context) {
    super(commandText, context);
    this.accountProvider = AccountProvider.ALIYUN;
    this.accessId = accessId;
    this.accessKey = accessKey;
  }

  @Override
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
    if (stsToken != null) {
      getContext().setStsToken(stsToken);
    }
  }

  /**
   * 通过传递的参数，解析出对应的command
   * **/
  public static LoginCommand parse(List<String> optionList, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    LoginCommand command;

    // 处理login, 这是默认的方式，走aliyun的签名认证
    if (optionList.contains(OPTION_USER) && optionList.contains(OPTION_PASSWORD)
        && optionList.indexOf(OPTION_USER) + 1 < optionList.size()
        && optionList.indexOf(OPTION_PASSWORD) + 1 < optionList.size()) {

      String uPara = optionList.get(optionList.indexOf(OPTION_USER) + 1);
      String pPara = optionList.get(optionList.indexOf(OPTION_PASSWORD) + 1);

      // 如果-u\-p后面还是命令，则是无效的命令
      if (uPara.startsWith("-") || pPara.startsWith("-")) {
        throw new ODPSConsoleException(ODPSConsoleConstants.BAD_COMMAND);
      }

      command = new LoginCommand(uPara, pPara, null, sessionContext);

      // 消费掉这一个参数
      optionList.remove(optionList.indexOf(OPTION_USER));
      optionList.remove(optionList.indexOf(OPTION_PASSWORD));
      optionList.remove(optionList.indexOf(uPara));
      optionList.remove(optionList.indexOf(pPara));

    } else {
      // Handle the login info based on the account provider.
      String accountProviderStr =
          ODPSConsoleUtils.shiftOption(optionList, LONG_OPTION_ACCOUNT_PROVIDER);

      if (accountProviderStr == null) {
        return null;
      }

      AccountProvider accountProvider = AccountProvider.valueOf(accountProviderStr.toUpperCase());
      command = new LoginCommand(accountProvider, null, sessionContext);
      switch (accountProvider) {
        case ALIYUN: {
          String accessId = ODPSConsoleUtils.shiftOption(optionList, LONG_OPTION_ACCESS_ID);
          String accessKey = ODPSConsoleUtils.shiftOption(optionList, LONG_OPTION_ACCESS_KEY);
          if (accessId == null || accessKey == null) {
            String errMsg = "Aliyun account requires accessKeyId and accessKeySecret.";
            throw new ODPSConsoleException(errMsg);
          }
          command.setAccessId(accessId);
          command.setAccessKey(accessKey);
          break;
        }
        case STS: {
          String accessId = ODPSConsoleUtils.shiftOption(optionList, LONG_OPTION_ACCESS_ID);
          String accessKey = ODPSConsoleUtils.shiftOption(optionList, LONG_OPTION_ACCESS_KEY);
          String stsToken = ODPSConsoleUtils.shiftOption(optionList, LONG_OPTION_STS_TOKEN);
          if (accessId == null || accessKey == null || stsToken == null) {
            String errMsg = "STS account requires accessKeyId, accessKeySecret and STS token";
            throw new ODPSConsoleException(errMsg);
          }
          command.setAccessId(accessId);
          command.setAccessKey(accessKey);
          command.setStsToken(stsToken);
          break;
        }
        case TAOBAO:
        case BEARER_TOKEN:
        default:
          throw new ODPSConsoleException(ODPSConsoleConstants.UNSUPPORTED_ACCOUNT_PROVIDER);
      }
    }

    return command;
  }
}
