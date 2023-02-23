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
import com.aliyun.odps.account.Account;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class KonfigFromSysEnvCommand extends AbstractCommand {
    public static final String OPTION_KONFIGFROMSYSENV = "-K";
    public static final String OPTION_KONFIGFROMSYSENV_LONG = "--konfig";

    // 与cloudshell中的环境变量名保持一致
    public static final String SYSENV_ACCESS_ID = "ACCESS_KEY_ID";
    public static final String SYSENV_ACCESS_KEY = "ACCESS_KEY_SECRET";
    public static final String SYSENV_SECURITY_TOKEN = "SECURITY_TOKEN";
    public static final String SYSENV_ALIBABA_CLOUD_ACCESS_ID = "ALIBABA_CLOUD_ACCESS_KEY_ID";
    public static final String SYSENV_ALIBABA_CLOUD_ACCESS_KEY = "ALIBABA_CLOUD_ACCESS_KEY_SECRET";
    public static final String SYSENV_ALIBABA_CLOUD_SECURITY_TOKEN = "ALIBABA_CLOUD_SECURITY_TOKEN";

    // odps特有的环境变量名
    public static final String SYSENV_APP_ACCESS_ID = "ODPS_CFG_APP_ACCESS_KEY_ID";
    public static final String SYSENV_APP_ACCESS_KEY = "ODPS_CFG_ACCESS_KEY_SECRET";
    public static final String SYSENV_PROJECT_NAME = "ODPS_CFG_PROJECTNAME";
    public static final String SYSENV_ODPS_ENDPOINT = "ODPS_CFG_ENDPOINT";

    private String accessID;
    private String accessKey;
    private String securityToken;

    private String odpsAppAccessID;
    private String odpsAppAccessKey;
    private String odpsProjectName;
    private String odpsEndpoint;


    public KonfigFromSysEnvCommand(String commandText, ExecutionContext context) {
        super(commandText, context);
        accessID = System.getenv(SYSENV_ACCESS_ID);

        if (!StringUtils.isEmpty(accessID)) {
            accessKey = System.getenv(SYSENV_ACCESS_KEY);
            securityToken = System.getenv(SYSENV_SECURITY_TOKEN);
        } else {
            accessID = System.getenv(SYSENV_ALIBABA_CLOUD_ACCESS_ID);
            accessKey = System.getenv(SYSENV_ALIBABA_CLOUD_ACCESS_KEY);
            securityToken = System.getenv(SYSENV_ALIBABA_CLOUD_SECURITY_TOKEN);
        }

        odpsAppAccessID = System.getenv(SYSENV_APP_ACCESS_ID);
        odpsAppAccessKey = System.getenv(SYSENV_APP_ACCESS_KEY);
        odpsProjectName = System.getenv(SYSENV_PROJECT_NAME);
        odpsEndpoint = System.getenv(SYSENV_ODPS_ENDPOINT);
    }

    protected void run() throws OdpsException, ODPSConsoleException {
        ExecutionContext execCtx = getContext();
        if (accessID == null || accessKey == null) {
            throw new ODPSConsoleException("Both ACCESS_KEY_ID and ACCESS_KEY_SECRET are required in sysenv.");
        }
        execCtx.setAccessId(accessID);
        execCtx.setAccessKey(accessKey);

        if (securityToken != null) {
            execCtx.setStsToken(securityToken);
            execCtx.setAccountProvider(Account.AccountProvider.STS);
        }

        if (odpsAppAccessID != null && odpsAppAccessKey != null) {
            execCtx.setAppAccessId(odpsAppAccessID);
            execCtx.setAppAccessKey(odpsAppAccessKey);
        }
        if (odpsProjectName != null) {
            execCtx.setProjectName(odpsProjectName);
        }
        if (odpsEndpoint != null) {
            execCtx.setEndpoint(odpsEndpoint);
        }
    }

    public static KonfigFromSysEnvCommand parse(List<String> optionList, ExecutionContext sessionContext)
            throws ODPSConsoleException {
        int optionPosKonfig = optionList.indexOf(OPTION_KONFIGFROMSYSENV);
        int optionPosKonfigLong = optionList.indexOf(OPTION_KONFIGFROMSYSENV_LONG);
        if ((optionPosKonfig != -1 && optionList.remove(optionPosKonfig) != null) ||
                (optionPosKonfigLong != -1 && optionList.remove(optionPosKonfigLong) != null)) {
            return new KonfigFromSysEnvCommand(optionPosKonfig != -1 ?
                    OPTION_KONFIGFROMSYSENV : OPTION_KONFIGFROMSYSENV_LONG, sessionContext);
        }
        return null;
    }
}