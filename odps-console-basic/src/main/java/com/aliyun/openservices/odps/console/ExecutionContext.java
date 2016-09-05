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

package com.aliyun.openservices.odps.console;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Properties;

import org.json.JSONException;
import org.json.JSONObject;

import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.ExtProperties;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

public class ExecutionContext implements Cloneable {

  // 默认为9，最低
  public final static Integer DEFAULT_PRIORITY = 9;

  private String projectName = "";
  private String endpoint = "";

  private String accessId = "";
  private String accessKey = "";
  private String proxyHost;
  private Integer proxyPort;
  private boolean debug = false;
  private boolean htmlMode = false;
  private boolean interactiveMode = false;

  private Double conformDataSize = null;

  public boolean isHtmlMode() {
    return htmlMode;
  }

  public void setHtmlMode(boolean htmlMode) {
    this.htmlMode = htmlMode;
  }

  public boolean isInteractiveMode() {
    return interactiveMode;
  }

  public void setInteractiveMode(boolean interactiveMode) {
    this.interactiveMode = interactiveMode;
  }

  // 帐号认证信息
  private String accessToken;
  private String accountProvider;

  // commands扩展
  private String userCommands;

  // 默认为9，最低
  private Integer priority = DEFAULT_PRIORITY;

  // dryrun模式
  private boolean isDryRun = false;

  private boolean isJson = false;

  private int consoleWidth = 150;
  private boolean isAsyncMode = false;
  private int step = 0;

  // 输出格式机器有读，当前在read命令中可以输出cvs格式
  private boolean machineReadable = false;

  // 设置retry的次数
  private int retryTimes = 1;

  // 定义post hooks class
  private String odpsHooks;

  private DefaultOutputWriter outputWriter = new DefaultOutputWriter(this);

  private String logViewHost;

  private String updateUrl;

  private boolean testEnv = false;

  private String tunnelEndpoint;
  private String datahubEndpoint;

  private String configFile;
  private String runningCluster;

  public boolean isHttpsCheck() {
    return httpsCheck;
  }

  public void setHttpsCheck(boolean httpsCheck) {
    this.httpsCheck = httpsCheck;
  }

  // 设置 是否检查 https policy
  private boolean httpsCheck = false;

  public boolean isTestEnv() {
    return testEnv;
  }

  public void setTestEnv(boolean testEnv) {
    this.testEnv = testEnv;
  }

  public String getUpdateUrl() {
    return updateUrl;
  }

  public void setUpdateUrl(String updateUrl) {
    this.updateUrl = updateUrl;
  }

  public ExecutionContext() {
  }

  public String getLogViewHost() {
    return logViewHost;
  }

  public void setLogViewHost(String logViewHost) {
    this.logViewHost = logViewHost;
  }

  public String getOdpsHooks() {
    return odpsHooks;
  }

  public void setOdpsHooks(String odpsHooks) {
    this.odpsHooks = odpsHooks;
  }

  public boolean isMachineReadable() {
    return machineReadable;
  }

  public void setMachineReadable(boolean machineReadable) {
    this.machineReadable = machineReadable;
  }

  public int getRetryTimes() {
    return retryTimes;
  }

  public void setRetryTimes(int retryTimes) {
    this.retryTimes = retryTimes;
  }

  public boolean isAsyncMode() {
    return isAsyncMode;
  }

  public void setAsyncMode(boolean isAsyncMode) {
    this.isAsyncMode = isAsyncMode;
  }

  public boolean isDryRun() {
    return isDryRun;
  }

  public void setDryRun(boolean isDryRun) {
    this.isDryRun = isDryRun;
  }

  public boolean isJson() {
    return isJson;
  }

  public void setJson(boolean isJson) {
    this.isJson = isJson;
  }

  public int getStep() {
    return step;
  }

  public void setStep(int step) {
    this.step = step;
  }

  public String getProjectName() {
    return projectName;
  }

  public void setProjectName(String projectName) {
    if (projectName != null) {
      this.projectName = projectName.trim();
    }
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    // end point的顺序是 env->parameter->config
    // 注释有2个 在云端要用 || pre mode 要用
    // TODO pre mode 已经不存在，这里需要打点
    String envEndPoint = System.getenv("ODPS_ENDPOINT");
    if (!StringUtils.isNullOrEmpty(envEndPoint)) {
      endpoint = envEndPoint;
    } else {
      if (endpoint != null) {
        this.endpoint = endpoint.trim();
      }
    }
  }

  public String getAccessId() {
    return accessId;
  }

  public void setAccessId(String accessId) {
    if (accessId != null) {
      this.accessId = accessId.trim();
    }
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    if (accessKey != null) {
      this.accessKey = accessKey.trim();
    }

  }

  public void setConfirmDataSize(Double cost) {
    this.conformDataSize = cost;
  }

  public Double getConfirmDataSize() {
    return this.conformDataSize;
  }

  public String getProxyHost() {
    return proxyHost;
  }

  public void setProxyHost(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  public Integer getProxyPort() {
    return proxyPort;
  }

  public void setProxyPort(Integer proxyPort) {
    this.proxyPort = proxyPort;
  }

  public int getConsoleWidth() {
    return consoleWidth;
  }

  public void setConsoleWidth(int consoleWidth) {
    this.consoleWidth = consoleWidth;
  }

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean isDebug) {
    this.debug = isDebug;
  }

  public DefaultOutputWriter getOutputWriter() {

    return outputWriter;
  }

  public void setOutputWriter(DefaultOutputWriter outputWriter) {
    this.outputWriter = outputWriter;
  }

  public Integer getPriority() {
    return priority;
  }

  public void setPriority(Integer priority) {
    this.priority = priority;
  }

  @Override
  public ExecutionContext clone() {

    try {
      return (ExecutionContext) super.clone();
    } catch (CloneNotSupportedException e) {
      //SUPPORTED, would not enter here.
      return null;
    }
  }

  /**
   * 从默认配置文件中加载运行配置。
   *
   * @throws ODPSConsoleException
   */
  public static ExecutionContext init() throws ODPSConsoleException {
    return load(null);
  }

  /**
   * 从配置文件中加载运行配置。
   *
   * @throws ODPSConsoleException
   */
  public static ExecutionContext load(String config) throws ODPSConsoleException {

    ExecutionContext context = new ExecutionContext();

    if (config != null)
    {
      config = FileUtil.expandUserHomeInPath(config);
    }

    // 从文件读取配置信息
    String configFile = (config != null ? config : ODPSConsoleUtils.getConfigFilePath());
    if (configFile == null) {
      return context;
    }

    try {
      // configFile如果有空格的话，File(configFile)是找不到的
      configFile = URLDecoder.decode(configFile, "utf-8");
    } catch (Exception e1) {
      // do nothing
    }

    File file = new File(configFile);
    if (!file.exists()) {
      // 如果文件不存，则不通过文件加载ExecutionContext
      // 用户可以通过命令行来设置ExecutionContext
      return context;
    }

    context.setConfigFile(configFile);

    FileInputStream configInputStream = null;
    try {
      configInputStream = new FileInputStream(configFile);

      Properties properties = new ExtProperties();
      properties.load(configInputStream);

      String projectName = properties.getProperty(ODPSConsoleConstants.PROJECT_NAME);
      String endpoint = properties.getProperty(ODPSConsoleConstants.END_POINT);
      String accessId = properties.getProperty(ODPSConsoleConstants.ACCESS_ID);
      String accessKey = properties.getProperty(ODPSConsoleConstants.ACCESS_KEY);
      String dataSizeConfirm = properties.getProperty(ODPSConsoleConstants.DATA_SIZE_CONFIRM);
      String host = properties.getProperty(ODPSConsoleConstants.PROXY_HOST);
      String port = properties.getProperty(ODPSConsoleConstants.PROXY_PORT);
      String hooks = properties.getProperty(ODPSConsoleConstants.POST_HOOK_CLASS);
      String tunnelEndpoint = properties.getProperty(ODPSConsoleConstants.TUNNEL_ENDPOINT);
      String datahubEndpoint = properties.getProperty(ODPSConsoleConstants.DATAHUB_ENDPOINT);
      String runningCluster = properties.getProperty(ODPSConsoleConstants.RUNNING_CLUSTER);
      String logViewHost = properties.getProperty(ODPSConsoleConstants.LOG_VIEW_HOST);
      context.setLogViewHost(logViewHost);

      context.setUpdateUrl(properties.getProperty("update_url"));

      context.setProxyHost(host);
      if (!StringUtils.isNullOrEmpty(port)) {
        context.setProxyPort(Integer.valueOf(port));
      }
      context.setAccessId(accessId);
      context.setAccessKey(accessKey);
      context.setEndpoint(endpoint);
      context.setProjectName(projectName);
      if (dataSizeConfirm != null) {
        Double value = Double.valueOf(dataSizeConfirm);
        if (value <= 0) {
          value = null;
        }
        context.setConfirmDataSize(value);
      }
      context.setOdpsHooks(hooks);
      context.setTunnelEndpoint(tunnelEndpoint);
      context.setDatahubEndpoint(datahubEndpoint);
      context.setRunningCluster(runningCluster);

      // load 用户定义的Interactive Command
      context.setUserCommands(properties.getProperty(ODPSConsoleConstants.USER_COMMANDS));

      // debug mode
      String is_debug = properties.getProperty(ODPSConsoleConstants.IS_DEBUG);
      if (is_debug != null) {
        context.setDebug(Boolean.valueOf(is_debug));
      }

      // console联接的是否为测试环境
      String testEnv = properties.getProperty("test_env");
      if (testEnv != null) {
        context.setTestEnv(Boolean.valueOf(testEnv));
      }
      context.setAccountProvider(properties.getProperty(ODPSConsoleConstants.ACCOUNT_PROVIDER));

      String instancePriority = properties.getProperty(ODPSConsoleConstants.INSTANCE_PRIORITY);
      if (instancePriority != null) {
        context.setPriority(Integer.valueOf(instancePriority));
      }

      String https_check = properties.getProperty(ODPSConsoleConstants.HTTPS_CHECK);
      if (https_check != null) {
        context.setHttpsCheck(Boolean.valueOf(https_check));
      }


      // 取console屏幕的宽度
      context.setConsoleWidth(ODPSConsoleUtils.getConsoleWidth());

    } catch (Exception e) {
      throw new ODPSConsoleException(ODPSConsoleConstants.LOAD_CONFIG_ERROR, e);
    } finally {

      if (configInputStream != null) {
        try {
          configInputStream.close();
        } catch (IOException e) {
        }
      }
    }

    return context;

  }

  public String getAccessToken() {
    return accessToken;
  }

  public void setAccessToken(String accessToken) {
    this.accessToken = accessToken;
  }

  public String getAccountProvider() {
    return accountProvider;
  }

  public void setAccountProvider(String accountProvider) {
    if (accountProvider != null) {
      this.accountProvider = accountProvider.trim();
    }
    this.accountProvider = accountProvider;
  }

  public String getUserCommands() {
    return userCommands;
  }

  public void setUserCommands(String userCommands) {
    this.userCommands = userCommands;
  }

  public String getTunnelEndpoint() {
    return tunnelEndpoint;
  }

  public void setTunnelEndpoint(String tunnelEndpoint) {
    this.tunnelEndpoint = tunnelEndpoint;
  }

  public String getDatahubEndpoint() {
    return datahubEndpoint;
  }

  public void setDatahubEndpoint(String datahubEndpoint) {
    this.datahubEndpoint = datahubEndpoint;
  }

  public String getConfigFile() {
    return configFile;
  }

  public void setConfigFile(String configFile) {
    this.configFile = configFile;
  }

  public JSONObject toJson() throws JSONException {
    JSONObject jsonObj = new JSONObject();
    jsonObj.put("project", projectName);
    jsonObj.put("endpoint", endpoint);
    jsonObj.put("proxyHost", proxyHost);
    jsonObj.put("proxyPort", proxyPort);
    jsonObj.put("debug", debug);
    jsonObj.put("accountProvider", accountProvider);
    jsonObj.put("userCommands", userCommands);
    jsonObj.put("priority", priority);
    jsonObj.put("isDryRun", isDryRun);
    jsonObj.put("isJson", isJson);
    jsonObj.put("consoleWidth", consoleWidth);
    jsonObj.put("isAsyncMode", isAsyncMode);
    jsonObj.put("step", step);
    jsonObj.put("machineReadable", machineReadable);
    jsonObj.put("retryTimes", retryTimes);
    jsonObj.put("odpsHooks", odpsHooks);
    jsonObj.put("logViewHost", logViewHost);
    jsonObj.put("accessToken", accessToken);
    jsonObj.put("tunnelEndpoint", tunnelEndpoint);
    jsonObj.put("runningCluster", runningCluster);
    jsonObj.put("https_check", httpsCheck);
    return jsonObj;
  }

  public void setRunningCluster(String runningCluster) {
    this.runningCluster = runningCluster;
  }

  public String getRunningCluster() {
    return runningCluster;
  }
}
