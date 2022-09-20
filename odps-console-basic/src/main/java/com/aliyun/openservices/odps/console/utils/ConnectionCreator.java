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

package com.aliyun.openservices.odps.console.utils;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsHook;
import com.aliyun.odps.OdpsHooks;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.AppAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.rest.RestClient.RetryLogger;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;

/**
 * 创建ODPSConnection
 *
 * @author shuman.gansm
 */
public class ConnectionCreator {

  public static class OdpsRetryLogger extends RetryLogger {

    @Override
    public void onRetryLog(Throwable e, long retryCount, long sleepTime) {
      if (e != null && e instanceof OdpsException) {
        String requestId = ((OdpsException) e).getRequestId();
        if (requestId != null) {
          System.err.println(String.format(
              "Warning: ODPS request failed, requestID:%s, retryCount:%d, will retry in %d seconds.",
              requestId, retryCount, sleepTime));
          return;
        }
      }
      System.err.println(String.format(
          "Warning: ODPS request failed:%s, retryCount:%d, will retry in %d seconds.", e.getMessage(),retryCount,
          sleepTime));
    }
  }

  private Account getAccount(ExecutionContext context) throws ODPSConsoleException {

    AccountProvider accountProvider = context.getAccountProvider();
    switch (accountProvider) {
      case ALIYUN:
        return new AliyunAccount(context.getAccessId(), context.getAccessKey());
      case STS:
        return new StsAccount(context.getAccessId(), context.getAccessKey(), context.getStsToken());
      default:
        throw new ODPSConsoleException("unsupport account provider:" + accountProvider);
    }
  }

  /**
   * Return the application account. If the application account is not set, return null.
   * @param context
   * @return {@link AppAccount}
   * @throws ODPSConsoleException
   */
  private AppAccount getAppAccount(ExecutionContext context) {
    String appAccessId = context.getAppAccessId();
    String appAccessKey = context.getAppAccessKey();
    if (!StringUtils.isNullOrEmpty(appAccessId) && !StringUtils.isNullOrEmpty(appAccessKey)) {
      return new AppAccount(new AliyunAccount(context.getAppAccessId(), context.getAppAccessKey()));
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public Odps create(ExecutionContext context) throws ODPSConsoleException {

    if (context == null) {
      throw new ODPSConsoleException(ODPSConsoleConstants.EXECUTIONCONTEXT_NOT_BE_SET);
    }

    if (context.getEndpoint() == null) {
      throw new ODPSConsoleException("pls set endpoint.");
    }

    String projectName = context.getProjectName();
    // String schemaName = context.getSchemaName();

    Account account = getAccount(context);
    AppAccount appAccount = getAppAccount(context);

    Odps odps = new Odps(account, appAccount);
    odps.setEndpoint(context.getEndpoint());
    if (StringUtils.isNullOrEmpty(projectName)) {
      odps.setDefaultProject(null);
    } else {
      odps.setDefaultProject(projectName);
    }
    //TODO schema need this??
    // odps.setCurrentSchema(schemaName);

    odps.setUserAgent(ODPSConsoleUtils.getUserAgent());

    if (!context.isHttpsCheck()) {
      odps.getRestClient().setIgnoreCerts(true);
    }

    odps.setLogViewHost(context.getLogViewHost());
    odps.getRestClient().setRetryLogger(new OdpsRetryLogger());
    odps.instances().setDefaultRunningCluster(context.getRunningCluster());
    OdpsHooks.getRegisteredHooks().clear();
    String hookString = context.getOdpsHooks();
    if (!StringUtils.isNullOrEmpty((hookString))) {
      String[] hooks = hookString.split(",");
      try {
        for (String hook : hooks) {
          Class<? extends OdpsHook> hookClass;
          hookClass = (Class<? extends OdpsHook>) Class.forName(hook);
          OdpsHooks.registerHook(hookClass);
        }
      } catch (ClassNotFoundException e) {
        throw new ODPSConsoleException("ClassNotFound : " + e.getMessage(), e);
      }
    }
    return odps;
  }
}
