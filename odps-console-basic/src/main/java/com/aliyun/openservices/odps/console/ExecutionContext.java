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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.odps.sqa.FallbackPolicy;
import com.aliyun.odps.sqa.SQLExecutor;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.output.InstanceRunner;
import com.aliyun.openservices.odps.console.utils.ExtProperties;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import com.aliyun.openservices.odps.console.utils.LocalCacheUtils;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class ExecutionContext implements Cloneable {

  // 默认为9，最低
  public final static Integer DEFAULT_PRIORITY = 9;

  // PAI任务优先级默认为9
  public final static Integer DEFAULT_PAI_PRIORITY = 9;

  // MCQA attach session timeout, unit: seconds
  public final static Long DEFAULT_ATTACH_SESSION_TIMEOUT = 60L;

  private static final String SET_COMMAND_PREFIX = "set.";

  private boolean initialized = false;

  private String projectName = "";
  private String schemaName = null;
  private String parseSchemaName = null;
  private String endpoint = "";

  private String quotaRegionId;
  private String quotaName;

  // 帐号认证信息
  private AccountProvider accountProvider = AccountProvider.ALIYUN;
  private String accessId = "";
  private String accessKey = "";
  private String stsToken = "";
  private String appAccessId = "";
  private String appAccessKey = "";
  private String proxyHost;
  private Integer proxyPort;
  private boolean debug = false;
  private boolean interactiveMode = false;
  private Long attachSessionTimeout = DEFAULT_ATTACH_SESSION_TIMEOUT;

  private Double conformDataSize = null;
  private boolean autoSessionMode = false;
  private String interactiveSessionName = "public.default";
  private LocalCacheUtils.CacheItem localCache = null;
  private boolean odpsNamespaceSchema = false;
  private boolean parseNamespaceSchema = false;

  public boolean isInteractiveOutputCompatible() {
    return interactiveOutputCompatible;
  }

  public void setInteractiveOutputCompatible(boolean interactiveOutputCompatible) {
    this.interactiveOutputCompatible = interactiveOutputCompatible;
  }

  private boolean interactiveOutputCompatible = false;

  public boolean isInteractiveMode() {
    return interactiveMode;
  }

  public void setInteractiveMode(boolean interactiveMode) {
    this.interactiveMode = interactiveMode;
  }

  // commands扩展
  private String userCommands;

  // 默认为9，最低
  private Integer priority = DEFAULT_PRIORITY;

  // PAI默认优先级为1, 最高
  private Integer paiPriority = DEFAULT_PAI_PRIORITY;

  // datetime 数据的时区属性
  // 可通过 set odps.sql.timezone=xxx 设置
  private String sqlTimezone = null;
  // 用户是否设置过sqlTimezone
  private boolean userSetTimezone = false;
  private String defaultSqlTimezone = TimeZone.getDefault().getID();

  // dryrun模式
  private boolean isDryRun = false;

  private boolean isJson = false;

  private int consoleWidth = 150;
  private boolean isAsyncMode = false;
  private boolean pMCMode = false;
  private int step = 0;

  // 输出格式机器有读，当前在read命令中可以输出cvs格式
  private boolean machineReadable = false;

  // 设置retry的次数
  private int retryTimes = 1;

  // 定义post hooks class
  private String odpsHooks;

  private DefaultOutputWriter outputWriter = new DefaultOutputWriter(this);

  private String logViewHost;
  private int logViewLife = 30 * 24; // hours

  private String updateUrl;

  private boolean testEnv = false;

  private String tunnelEndpoint;
  private String datahubEndpoint;

  private String configFile;
  private String runningCluster;

  private boolean isInteractiveQuery;

  private Map<String, String> predefinedSetCommands = new HashMap<String, String>();

  private static InstanceRunner instanceRunner;
  // session query executor
  private static SQLExecutor executor;

  public boolean isHttpsCheck() {
    return httpsCheck;
  }

  public void setHttpsCheck(boolean httpsCheck) {
    this.httpsCheck = httpsCheck;
  }

  // 设置 是否检查 https policy
  private boolean httpsCheck = false;

  // 设置 是否使用 instance tunnel 来下载 sql 数据
  private boolean useInstanceTunnel = false;
  private Long instanceTunnelMaxRecord = null;
  //Uint : Bytes
  private Long instanceTunnelMaxSize = null;

  private FallbackPolicy fallbackPolicy = FallbackPolicy.alwaysFallbackExceptAttachPolicy();

  // Cupid proxy 泛域名
  private String odpsCupidProxyEndpoint;

  private Long maxAttachSessionInstances = null;
  /**
   * In lite mode, the end to end execution time is optimized by:
   *   1. Skip the instance summary
   */
  private boolean liteMode;


  /**
   * when this flag is true, [use project xxx] command with default trigger [use project xxx --with-settings]
   */
  private boolean useProjectWithSettings = false;

  public boolean isInitialized() {
    return initialized;
  }

  public void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

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

  // hours
  public int getLogViewLife() {
    return logViewLife;
  }

  // hours
  public void setLogViewLife(int i) {
    this.logViewLife = i;
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

  public boolean isPMCMode() {
    return pMCMode;
  }

  public void setPMCMode(boolean pmcMode) {
    this.pMCMode = pmcMode;
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

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getParseSchemaName() {
    return parseSchemaName;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    if (endpoint != null) {
      this.endpoint = endpoint.trim();
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

  public String getAppAccessId() {
    return appAccessId;
  }

  public void setAppAccessId(String appAccessId) {
    if (appAccessId != null) {
      this.appAccessId = appAccessId;
    }
  }

  public String getAppAccessKey() {
    return appAccessKey;
  }

  public void setAppAccessKey(String appAccessKey) {
    if (appAccessKey != null) {
      this.appAccessKey = appAccessKey;
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

  public Integer getPaiPriority() {
    return paiPriority;
  }

  public void setPriority(Integer priority) {
    this.priority = priority;
  }

  public void setPaiPriority(Integer paiPriority) {
    this.paiPriority = paiPriority;
  }

  public void setSqlTimezone(String timezone) {
    this.sqlTimezone = timezone;
  }

  public void setDefaultSqlTimezone(String defaultSqlTimezone) {
    this.defaultSqlTimezone = defaultSqlTimezone;
  }

  public String getDefaultSqlTimezone() {
    return defaultSqlTimezone;
  }

  public void setUserSetSqlTimezone(boolean userSetTimezone) {
    this.userSetTimezone = userSetTimezone;
  }

  public boolean isUserSetSqlTimezone() {
    return this.userSetTimezone;
  }

  public String getSqlTimezone() {
    return this.sqlTimezone;
  }

  public void setInteractiveQuery(boolean isInteractive) {
    this.isInteractiveQuery = isInteractive;
  }

  public boolean isInteractiveQuery() {
    return this.isInteractiveQuery;
  }

  public void setAttachTimeout(Long timeout) {
    this.attachSessionTimeout = timeout;
  }

  public Long getAttachTimeout() {
    return attachSessionTimeout;
  }

  public Map<String, String> getPredefinedSetCommands() {
    return predefinedSetCommands;
  }

  public static void setInstanceRunner(InstanceRunner runner) {
    instanceRunner = runner;
  }

  public static InstanceRunner getInstanceRunner() {
    return instanceRunner;
  }

  public String getOdpsCupidProxyEndpoint(){ return this.odpsCupidProxyEndpoint; }

  private void setOdpsCupidProxyEndpoint(String odpsCupidProxyEndpoint){ this.odpsCupidProxyEndpoint = odpsCupidProxyEndpoint; }

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

    if (config != null) {
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
      // 如果文件不存在，则不通过文件加载ExecutionContext
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
      //TODO schema support in config.ini?
      String schemaName = properties.getProperty(ODPSConsoleConstants.SCHEMA_NAME);
      String endpoint = properties.getProperty(ODPSConsoleConstants.END_POINT);
      String accessId = properties.getProperty(ODPSConsoleConstants.ACCESS_ID);
      String accessKey = properties.getProperty(ODPSConsoleConstants.ACCESS_KEY);
      String appAccessId = properties.getProperty(ODPSConsoleConstants.APP_ACCESS_ID);
      String appAccessKey = properties.getProperty(ODPSConsoleConstants.APP_ACCESS_KEY);
      String dataSizeConfirm = properties.getProperty(ODPSConsoleConstants.DATA_SIZE_CONFIRM);
      String host = properties.getProperty(ODPSConsoleConstants.PROXY_HOST);
      String port = properties.getProperty(ODPSConsoleConstants.PROXY_PORT);
      String hooks = properties.getProperty(ODPSConsoleConstants.POST_HOOK_CLASS);
      String tunnelEndpoint = properties.getProperty(ODPSConsoleConstants.TUNNEL_ENDPOINT);
      String datahubEndpoint = properties.getProperty(ODPSConsoleConstants.DATAHUB_ENDPOINT);
      String runningCluster = properties.getProperty(ODPSConsoleConstants.RUNNING_CLUSTER);
      String logViewHost = properties.getProperty(ODPSConsoleConstants.LOG_VIEW_HOST);
      String logViewLife = properties.getProperty(ODPSConsoleConstants.LOG_VIEW_LIFE);
      String updateUrl = properties.getProperty(ODPSConsoleConstants.UPDATE_URL);
      String odpsCupidProxyEndpoint = properties.getProperty(ODPSConsoleConstants.CUPID_PROXY_END_POINT);
      String interactiveSessionMode = properties.getProperty(ODPSConsoleConstants.ENABLE_INTERACTIVE_MODE);
      String interactiveSessionName = properties.getProperty(ODPSConsoleConstants.INTERACTIVE_SERVICE_NAME);
      String interactiveOutputCompatible = properties.getProperty(ODPSConsoleConstants.INTERACTIVE_OUTPUT_COMPATIBLE);

      context.setOdpsCupidProxyEndpoint(odpsCupidProxyEndpoint);

      context.setLogViewHost(logViewHost);
      if (!StringUtils.isNullOrEmpty(logViewLife)) {
        context.setLogViewLife(Integer.parseInt(logViewLife));
      }
      context.setUpdateUrl(updateUrl);

      context.setProxyHost(host);
      if (!StringUtils.isNullOrEmpty(port)) {
        context.setProxyPort(Integer.valueOf(port));
      }
      context.setAccessId(accessId);
      context.setAccessKey(accessKey);
      context.setAppAccessId(appAccessId);
      context.setAppAccessKey(appAccessKey);
      context.setEndpoint(endpoint);
      context.setProjectName(projectName);
      //TODO schema support in config.ini?
      // context.setSchemaName(schemaName);

      if (!StringUtils.isNullOrEmpty(dataSizeConfirm)) {
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
      String isDebug = properties.getProperty(ODPSConsoleConstants.IS_DEBUG);
      if (isDebug != null) {
        context.setDebug(Boolean.valueOf(isDebug));
      }

      // console联接的是否为测试环境
      String testEnv = properties.getProperty("test_env");
      if (testEnv != null) {
        context.setTestEnv(Boolean.valueOf(testEnv));
      }

      // Account provider
      String accountProviderStr = properties.getProperty(ODPSConsoleConstants.ACCOUNT_PROVIDER);
      if (accountProviderStr != null) {
        context.setAccountProvider(AccountProvider.valueOf(accountProviderStr.toUpperCase()));
      }

      String instancePriority = properties.getProperty(ODPSConsoleConstants.INSTANCE_PRIORITY);
      if (instancePriority != null) {
        context.setPriority(Integer.valueOf(instancePriority));
        context.setPaiPriority(Integer.valueOf(instancePriority));
      }

      String httpsCheck = properties.getProperty(ODPSConsoleConstants.HTTPS_CHECK);
      if (httpsCheck != null) {
        context.setHttpsCheck(Boolean.valueOf(httpsCheck));
      }

      String useInstanceTunnel = properties.getProperty(ODPSConsoleConstants.USE_INSTANCE_TUNNEL);
      if (!StringUtils.isNullOrEmpty(useInstanceTunnel)) {
        context.setUseInstanceTunnel(Boolean.valueOf(useInstanceTunnel));
      }

      String maxAttachSessionInstances =
          properties.getProperty(ODPSConsoleConstants.INTERACTIVE_MAX_ATTACH);
      if (!StringUtils.isNullOrEmpty(maxAttachSessionInstances)) {
        Long num = Long.parseLong(maxAttachSessionInstances);
        if (num <= 0) {
          num = null;
        }
        context.setMaxAttachSessionInstances(num);
      }

      String attachSessionTimeout = properties.getProperty(ODPSConsoleConstants.ATTACH_SESSION_TIMEOUT);
      if (!StringUtils.isNullOrEmpty(attachSessionTimeout)) {
        context.setAttachTimeout(Long.valueOf(attachSessionTimeout));
      }

      String instanceTunnelMaxRecord =
          properties.getProperty(ODPSConsoleConstants.INSTANCE_TUNNEL_MAX_RECORD);
      if (!StringUtils.isNullOrEmpty(instanceTunnelMaxRecord)) {
        Long num = Long.parseLong(instanceTunnelMaxRecord);
        if (num <= 0) {
          num = null;
        }
        context.setInstanceTunnelMaxRecord(num);
      }
      String instanceTunnelMaxSize =
          properties.getProperty(ODPSConsoleConstants.INSTANCE_TUNNEL_MAX_SIZE);
      if (!StringUtils.isNullOrEmpty(instanceTunnelMaxSize)) {
        Long size = Long.parseLong(instanceTunnelMaxSize);
        if (size <= 0) {
          size = null;
        }
        context.setInstanceTunnelMaxSize(size);
      }
      String sessionAutoRerun = properties.getProperty(ODPSConsoleConstants.INTERACTIVE_AUTO_RERUN);
      if (!StringUtils.isNullOrEmpty(sessionAutoRerun)) {
        if (Boolean.valueOf(sessionAutoRerun)) {
          context.fallbackPolicy = FallbackPolicy.alwaysFallbackExceptAttachPolicy();
        } else {
          context.fallbackPolicy = FallbackPolicy.nonFallbackPolicy();
        }
      }
      if (!StringUtils.isNullOrEmpty(interactiveSessionMode) && Boolean.valueOf(interactiveSessionMode)) {
        context.setAutoSessionMode(true);
        if (!StringUtils.isNullOrEmpty(interactiveSessionName)) {
           context.setInteractiveSessionName(interactiveSessionName);
        }
        if (!StringUtils.isNullOrEmpty(maxAttachSessionInstances)) {
          LocalCacheUtils.enableMultiAttachSessionMode(context.getMaxAttachSessionInstances());
          int maxRetry = 10;
          boolean readCacheSucc = false;
          while (maxRetry-- > 0) {
            try {
              // load once from file, will use .session/${config_hash} as session dir
              LocalCacheUtils.setCacheDir(configFile, endpoint, projectName, accessId);
              context.localCache = LocalCacheUtils.readCache();
              readCacheSucc = true;
              break;
            } catch (Exception e) {
              Thread.sleep(200);
            }
          }
          if (!readCacheSucc) {
            throw new ODPSConsoleException("Try to find available attach session failed after retry");
          }
        } else {
          // load once from file, will use .session/${config_hash} as session dir
          LocalCacheUtils.setCacheDir(configFile, endpoint, projectName, accessId);
          context.localCache = LocalCacheUtils.readCache();
        }
      }
      String sessionOutputCompatible = properties.getProperty(ODPSConsoleConstants.INTERACTIVE_OUTPUT_COMPATIBLE);
      if (!StringUtils.isNullOrEmpty(sessionOutputCompatible)) {
        if (Boolean.valueOf(sessionOutputCompatible)) {
          context.interactiveOutputCompatible = true;
        } else {
          context.interactiveOutputCompatible = false;
        }
      }

      String liteMode = properties.getProperty(ODPSConsoleConstants.LITE_MODE);
      if (!StringUtils.isNullOrEmpty(liteMode)) {
        context.setLiteMode(Boolean.valueOf(liteMode));
      }

      // 取console屏幕的宽度
      context.setConsoleWidth(ODPSConsoleUtils.getConsoleWidth());

      // load predefined set commands, will execute them later
      for (String propertyName : properties.stringPropertyNames()) {
        if (propertyName.toLowerCase().startsWith(SET_COMMAND_PREFIX)) {
          String key = propertyName.substring(SET_COMMAND_PREFIX.length()).trim();
          String value = properties.getProperty(propertyName).trim();
          context.predefinedSetCommands.put(key, value);
        }
      }

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

  private void setInteractiveSessionName(String interactiveSessionName) {
    this.interactiveSessionName = interactiveSessionName;
  }

  public String getStsToken() {
    return stsToken;
  }

  public void setStsToken(String stsToken) {
    this.stsToken = stsToken;
  }

  public AccountProvider getAccountProvider() {
    return accountProvider;
  }

  public void setAccountProvider(AccountProvider accountProvider) {
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

  public JsonObject toJson() throws JsonParseException {
    JsonObject jsonObj = new JsonObject();
    jsonObj.addProperty("project", projectName);
    jsonObj.addProperty("endpoint", endpoint);
    jsonObj.addProperty("proxyHost", proxyHost);
    jsonObj.addProperty("proxyPort", proxyPort);
    jsonObj.addProperty("debug", debug);
    jsonObj.addProperty("accountProvider", accountProvider.toString());
    jsonObj.addProperty("userCommands", userCommands);
    jsonObj.addProperty("priority", Integer.toString(priority)); // for old mr use getString to parse it.
    jsonObj.addProperty("isDryRun", isDryRun);
    jsonObj.addProperty("isJson", isJson);
    jsonObj.addProperty("consoleWidth", consoleWidth);
    jsonObj.addProperty("isAsyncMode", isAsyncMode);
    jsonObj.addProperty("isPMCMode", pMCMode);
    jsonObj.addProperty("step", step);
    jsonObj.addProperty("machineReadable", machineReadable);
    jsonObj.addProperty("retryTimes", retryTimes);
    jsonObj.addProperty("odpsHooks", odpsHooks);
    jsonObj.addProperty("logViewHost", logViewHost);
    jsonObj.addProperty("logViewLife", logViewLife);
    jsonObj.addProperty("tunnelEndpoint", tunnelEndpoint);
    jsonObj.addProperty("runningCluster", runningCluster);
    jsonObj.addProperty("https_check", httpsCheck);
    return jsonObj;
  }

  public void setRunningCluster(String runningCluster) {
    this.runningCluster = runningCluster;
  }

  public String getRunningCluster() {
    return runningCluster;
  }

  public void setUseInstanceTunnel(boolean useInstanceTunnel) {
    this.useInstanceTunnel = useInstanceTunnel;
  }

  public boolean isUseInstanceTunnel() {
    return useInstanceTunnel;
  }

  public void setInstanceTunnelMaxRecord(Long number) {
    this.instanceTunnelMaxRecord = number;
  }

  public void setMaxAttachSessionInstances(Long maxAttachSessionInstances) {
    this.maxAttachSessionInstances = maxAttachSessionInstances;
  }

  public Long getMaxAttachSessionInstances() {
    return maxAttachSessionInstances;
  }

  public void setInstanceTunnelMaxSize(Long size) {
    this.instanceTunnelMaxSize = size;
  }

  public Long getInstanceTunnelMaxRecord() {
    return instanceTunnelMaxRecord;
  }

  public Long getInstanceTunnelMaxSize() {
    return instanceTunnelMaxSize;
  }

  public FallbackPolicy getFallbackPolicy() {
    return fallbackPolicy;
  }

  public void setAutoSessionMode(boolean b) {
    autoSessionMode = b;
  }

  public boolean getAutoSessionMode() {
    return autoSessionMode;
  }

  public String getInteractiveSessionName() {
    return interactiveSessionName;
  }

  public LocalCacheUtils.CacheItem getLocalCache() {
    return localCache;
  }

  public static SQLExecutor getExecutor() {
    return executor;
  }

  public static void setExecutor(SQLExecutor executor) {
    ExecutionContext.executor = executor;
  }

  public boolean setLocalCache(LocalCacheUtils.CacheItem localCache) {
    this.localCache = localCache;
    // update file cache
    try {
      LocalCacheUtils.writeCache(this.localCache);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  // key: class.getSimpleName value: class.getName
  public static Map<String, String> commandBeforeHook = new HashMap<>();

  public boolean isLiteMode() {
    return liteMode;
  }

  public void setLiteMode(boolean liteMode) {
    this.liteMode = liteMode;
  }

  public String getQuotaName() {
    return quotaName;
  }

  public void setQuotaName(String quotaName) {
    this.quotaName = quotaName;
  }

  public String getQuotaRegionId() {
    return quotaRegionId;
  }

  public void setQuotaRegionId(String quotaRegionId) {
    this.quotaRegionId = quotaRegionId;
  }

  public void print() {
    DefaultOutputWriter writer = this.getOutputWriter();

    String endpoint = this.getEndpoint();
    String projectName = this.getProjectName();
    writer.writeErrorFormat(
        "Endpoint: %s\nProject: %s\n",
        endpoint,
        projectName);
    if (isSchemaMode()) {
      writer.writeErrorFormat("Schema: %s\n", ODPSConsoleUtils.getDisplaySchema(this));
    }
    String quotaRegionId = this.getQuotaRegionId();
    String quotaName = this.getQuotaName();
    writer.writeErrorFormat(
        "Quota: %s in region %s\n",
        org.apache.commons.lang.StringUtils.defaultIfEmpty(quotaName, "default"),
        org.apache.commons.lang.StringUtils.defaultIfEmpty(quotaRegionId, "N/A"));
    String timezone = this.getSqlTimezone();
    writer.writeErrorFormat(
        "Timezone: %s\n",
        org.apache.commons.lang.StringUtils.defaultIfEmpty(timezone, "default"));
  }

  public boolean isOdpsNamespaceSchema() {
    return odpsNamespaceSchema;
  }

  public boolean isProjectMode() {
    return !odpsNamespaceSchema;
  }

  public boolean isSchemaMode() {
    return odpsNamespaceSchema;
  }

  public void setOdpsNamespaceSchema(boolean odpsNamespaceSchema) {
    this.odpsNamespaceSchema = odpsNamespaceSchema;
  }

  public boolean isUseProjectWithSettings() {
    return useProjectWithSettings;
  }

  public void setUseProjectWithSettings(boolean useProjectWithSettings) {
    this.useProjectWithSettings = useProjectWithSettings;
  }
}
