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

package com.aliyun.openservices.odps.console.xflow;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.aliyun.odps.*;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.utils.GsonObjectBuilder;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.utils.QueryUtil;
import com.google.gson.GsonBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringEscapeUtils;

import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.XFlows.XFlowInstance;
import com.aliyun.odps.XFlows.XResult;
import com.aliyun.odps.utils.StringUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.output.DefaultOutputWriter;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.PluginUtil;
import com.aliyun.openservices.odps.console.utils.antlr.AntlrObject;

import org.jline.reader.UserInterruptException;

public class PAICommand extends AbstractCommand {

  private static final String TEMP_RESOURCE_PREFIX = "file:";
  private static List<String> printResultList = new ArrayList<String>();
  private static List<String> printUrlList = new ArrayList<String>();
  private static List<String> printUrlHosts = new ArrayList<String>();

  private static final int MAX_RETRY_TIMES = 10;
  private static final int RESUBMIT_INTERVAL_MS = 120 * 1000;

  @SuppressWarnings("static-access")
  private static Options initOptions() {
    Options opts = new Options();
    Option name = new Option("name", true, "model name");
    name.setRequired(true);
    Option project = new Option("project", true, "model project");
    Option property = OptionBuilder.withArgName("property=value").hasArgs(2).withValueSeparator()
        .withDescription("use value for given property").create("D");

    Option costFlag = new Option("cost", false,"cost mode");
    Option jobName = new Option("jobname", true, "user customized jobname");
    Option alinkVersion = new Option("alink_version", true, "alink version");
    opts.addOption(name);
    opts.addOption(project);
    opts.addOption(property);
    opts.addOption(costFlag);
    opts.addOption(jobName);
    opts.addOption(alinkVersion);
    return opts;
  }

  public static CommandLine getCommandLine(String commandText) throws ODPSConsoleException {
    AntlrObject antlr = new AntlrObject(commandText);
    String[] parts = antlr.getTokenStringArray();
    List<String> args = new ArrayList<String>();

    for (int i = 0; i < parts.length; i++) {
      String curr = parts[i];

      // In current antlr logic
      // -a=b   will be parsed to -a=b
      // -a="b" will be parsed to -a= and "b"
      String[] keyValue = curr.split("=", 2);
      if (keyValue.length == 2) {
        String value = keyValue[1];
        if (keyValue[1].isEmpty() && i+1 < parts.length) {
          value = parts[++i];
        }
        if (value.length() >= 2 &&
            (value.startsWith("\"") && value.endsWith("\"") ||
             value.startsWith("'") && value.endsWith("'"))) {
          value = value.substring(1, value.length() - 1);
        }
        args.add(keyValue[0] + "=" + StringEscapeUtils.unescapeJava(value));
      } else {
        args.add(parts[i]);
      }
    }

    if (args.size() < 2) {
      printUsage(System.err);
      throw new ODPSConsoleException("Invalid parameters - Generic options must be specified.");
    }

    Options opts = initOptions();
    CommandLineParser clp = new GnuParser();
    CommandLine cl;
    try {
      cl = clp.parse(opts, args.toArray(new String[]{}), false);
    } catch (Exception e) {
      throw new ODPSConsoleException("Unknown exception from client - " + e.getMessage(), e);
    }
    if (!cl.hasOption("name")) {
      throw new ODPSConsoleException(
          "Invalid parameters - model name must be specified, using '-t' option.");
    }

    if (cl.getArgList().size() > 1) {
      // only need arg 'PAI'
      throw new ODPSConsoleException("Invalid parameters - should use -D options.");
    }

    return cl;
  }

  public static final String[] HELP_TAGS = new String[]{"pai"};

  public static void printUsage(PrintStream stream) {
    stream.println("PAI –name <algo_name> [-cost] [-jobname <jobname>] -project <algo_src_project> -D<key>=<value> …");
  }

  public PAICommand(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {
    super(commandString, sessionContext);

  }

  static {
    try{
      Properties properties = PluginUtil.getPluginProperty(PAICommand.class);
      String cmd = properties.getProperty("print_result_list");
      if (!StringUtils.isNullOrEmpty(cmd)) {
        printResultList = Arrays.asList(cmd.split(","));
      }
      cmd = properties.getProperty("poll_url_list");
      if (!StringUtils.isNullOrEmpty(cmd)) {
        printUrlList = Arrays.asList(cmd.split(","));
      }
      cmd = properties.getProperty("poll_url_hosts");
      if (!StringUtils.isNullOrEmpty(cmd)) {
        printUrlHosts = Arrays.asList(cmd.split(","));
      }
    } catch (IOException e) {
      //do nothing
    }

  }

  public static HashMap<String, String> getUserConfig(CommandLine cl) {
    // get session config
    HashMap<String, String> userConfig = new HashMap<String, String>();

    String jobName = cl.getOptionValue("jobname");
    HashMap<String, String> settings
        = new HashMap<String, String>(SetCommand.setMap);

    if (jobName != null) {
      settings.put(
          "odps.task.workflow.custom_job_name", jobName);
    }

    if (!settings.isEmpty()) {
      userConfig.put("settings", new GsonBuilder().disableHtmlEscaping().create()
              .toJson(settings));
    }
    return userConfig;
  }

  private XFlowInstance CreateXflowInstance(Odps odps, CommandLine cl,
      StringBuilder urlBuilder) throws OdpsException, ODPSConsoleException{
    String algoName = cl.getOptionValue("name");
    String projectName = cl.getOptionValue("project");
    Properties properties = cl.getOptionProperties("D");

    String runningProject = odps.getDefaultProject();
    replaceProperty(properties, runningProject);

    XFlowInstance xFlowInstance = new XFlowInstance();
    xFlowInstance.setXflowName(algoName);

    if (projectName == null) {
      xFlowInstance.setProject("algo_public");
    } else {
      xFlowInstance.setProject(projectName);
    }



    String guid = UUID.randomUUID().toString();
    if (printUrlList.contains(algoName.toUpperCase())) {
      final String token = properties.getProperty("token");
      final String host = properties.getProperty("host");
      String defaultToken = new SimpleDateFormat("yyyyMMddHHmmss").format(
          new Date());
      defaultToken += "xflowinstance";
      defaultToken += guid.replaceAll("-", "");
      final String defaultHost = printUrlHosts.get(
          printUrlList.indexOf(algoName.toUpperCase()));
      urlBuilder.append("http://");
      if (token == null) {
        urlBuilder.append(defaultToken);
      } else {
        urlBuilder.append(token);
      }
      if (host == null) {
        urlBuilder.append("." + defaultHost);
      } else {
        urlBuilder.append("." + host);
      }

      if (token == null) {
        xFlowInstance.setParameter("token", defaultToken);
      }

      if (host == null) {
        xFlowInstance.setParameter("host", defaultHost);
      }
    }

    for (Entry<Object, Object> property : properties.entrySet()) {
      String value = property.getValue().toString();
      if (value.toLowerCase().startsWith(TEMP_RESOURCE_PREFIX)) {
        try {
          value = new URL(URLDecoder.decode(value, "utf-8")).getPath();
        } catch (IOException e) {
          throw new ODPSConsoleException("Invalid temp fileName:" + e.getMessage(), e);
        }
        value = odps.resources().createTempResource(runningProject, value).getName();
      }
      xFlowInstance.setParameter(property.getKey().toString(), value);
    }

    xFlowInstance.setGuid(guid);

    Integer priority = getContext().getPaiPriority();
    xFlowInstance.setPriority(priority);

    HashMap<String, String> userConfig = getUserConfig(cl);
    for (Entry<String, String> property : userConfig.entrySet()) {
      xFlowInstance.setProperty(property.getKey(), property.getValue());
    }

    return xFlowInstance;
  }

  /*
  * pai cost的实现里面, 有可能会返回除Input和WorkerNumber外的其他指标,如cpu, mem
  * 目前的cost只需要Input和WorkerNumber, 因此对cost输出做了限制, 要求两个指标同时存在
  * 否则提示错误
  *
  **/
  private void WriteEstimateResult(String estimateResult) {
    DefaultOutputWriter outputWriter = getContext().getOutputWriter();
    Map cost = GsonObjectBuilder.get().fromJson(estimateResult, Map.class);
    Map<String, Object> kvs = (Map) ((Map) cost.get("Cost")).get("PAI");

    String outputStr = "";
    for (Map.Entry<String, Object> entry : kvs.entrySet()) {
      if ("Input".equalsIgnoreCase(entry.getKey())) {
        if (entry.getValue().toString().isEmpty()) {
          outputWriter.writeError("Estimate Failed, empty Input returned.");
          return;
        }
        outputStr += String.format("%s:%s Bytes\n", entry.getKey(), entry.getValue());
      } else if ("WorkerNumber".equalsIgnoreCase(entry.getKey())) {
        if (entry.getValue().toString().isEmpty()) {
          outputWriter.writeError("Estimate Failed, empty WorkerNumber returned.");
          return;
        }
        outputStr += String.format("%s:%s", entry.getKey(), entry.getValue());
      }
    }
    outputWriter.writeResult(outputStr);
  }

  private void runInCostMode(CommandLine cl) throws OdpsException, ODPSConsoleException{
    Odps odps = getCurrentOdps();
    StringBuilder urlBuilder = new StringBuilder();
    XFlowInstance xFlowInstance = CreateXflowInstance(odps, cl, urlBuilder);

    // add cost flag
    xFlowInstance.setRunningMode("estimate");

    XFlows xFlows = odps.xFlows();
    Instance xInstance = xFlows.execute(xFlowInstance);
    System.err.println("ID = " + xInstance.getId());

    Map<String, String> results = xInstance.getTaskResults();
    if (results.containsKey("EstimateResult")) {
      String resultStr = results.get("EstimateResult");
      WriteEstimateResult(resultStr);
    } else {
      throw new OdpsException("Estimate Failed : This algorithm doesn't support cost estimation.");
    }
  }

  private void runNormally(CommandLine cl) throws OdpsException, ODPSConsoleException {
    Odps odps = getCurrentOdps();
    StringBuilder urlBuilder = new StringBuilder();
    XFlowInstance xFlowInstance = null;
    AlinkAdapter alinkAdapter = new AlinkAdapter(getContext(), odps, cl);
    if (alinkAdapter.needTransform()) {
      System.err.println("Begin create alink xflow instance");
      xFlowInstance = alinkAdapter.createAlinkXflowInstance();
    } else {
      xFlowInstance = CreateXflowInstance(odps, cl, urlBuilder);
    }

    Instance xInstance = runWithRetry(xFlowInstance, odps);
    System.err.println("ID = " + xInstance.getId());

    waitForCompletion(xInstance, odps, getContext(), urlBuilder.toString());
  }

  private Instance runWithRetry(XFlowInstance xFlowInstance, Odps odps) throws OdpsException {
    try {
      Instance xInstance = odps.xFlows().execute(xFlowInstance);
      return xInstance;
    } catch (OdpsException oe) {
      if (!needRetry(oe)) {
        throw oe;
      }
    }

    // use xflowInstance guid to check current xFlowInstance submitted or not
    Instance instance = null;
    try {
      // before retry, wait for several seconds
      Thread.sleep(TimeUnit.SECONDS.toMillis(10));
      instance = SeekInstanceByGuid(odps, xFlowInstance.getGuid());
    } catch (InterruptedException ie) {
      throw new UserInterruptException(ie.getMessage());
    } catch (Exception e) {
      throw new OdpsException("无法获取ID，请稍后重试。[" + e.getMessage() + "]", e);
    }

    if (instance != null) {
      return instance;
    }

    getWriter().writeError("Create PAI instance failed, will retry after " +
            String.valueOf(RESUBMIT_INTERVAL_MS / 1000) + "s");
    try {
      Thread.sleep(RESUBMIT_INTERVAL_MS);
    } catch (InterruptedException ie) {
      throw new UserInterruptException(ie.getMessage());
    }
    return odps.xFlows().execute(xFlowInstance);
  }

  private boolean needRetry(OdpsException e) {
    if (e.getCause() != null && e.getCause() instanceof IOException) {
      // for IOException, will retry
      return true;
    }

    String[] errorCodes = {"ODPS-1230153"};
    String errorMsg = e.getMessage();

    for (String ec : errorCodes) {
      // if response message inculde error code
      if (errorMsg.indexOf(ec) >= 0) {
        getWriter().writeDebug(e.getMessage() + ", reconnecting");
        return true;
      }
    }
    return false;
  }

  private Instance SeekInstanceByGuid(Odps odps, String guid) throws Exception {
    Odps newOdps = odps.clone();
    newOdps.getRestClient().setRetryTimes(0);
    String resource = ResourceBuilder.buildProjectResource(newOdps.getDefaultProject());
    Response response = newOdps.getRestClient().request(resource, "GET", null, null, null);
    String date = response.getHeader("Date");
    if (date == null) {
      date = DateUtils.formatRfc822Date(new Date());
    }
    Date sDate = DateUtils.parseRfc822Date(date);
    final Date fromDate = new Date(sDate.getTime() - 300 * 1000);
    final Date toDate = new Date(sDate.getTime() + 10 * 1000);
    InstanceFilter filter = new InstanceFilter();
    filter.setFromTime(fromDate);
    filter.setEndTime(toDate);
    Iterator<Instance> iterator = newOdps.instances().iterator(filter);
    while (iterator.hasNext()) {
      Instance instance = iterator.next();
      try {
        XFlowInstance xinstance = newOdps.xFlows().getXFlowInstance(instance);
        if (guid.equals(xinstance.getGuid())) {
          return instance;
        }
      } catch (OdpsException e) {
        // not a xflow instance, just ignore
        continue;
      }
    }
    return null;
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {

    CommandLine cl = getCommandLine(getCommandText());
    boolean hasCostOption = cl.hasOption("cost");

    // run xflow algorithms
    if (hasCostOption) {
      runInCostMode(cl);
    } else {
      runNormally(cl);
    }

  }

  public static void waitForInstanceTerminate(XFlowProgressHelper progressHelper,
                                              Set<String> logviewHasPrintSet, Instance xInstance,
                                              Odps odps, ExecutionContext context)
      throws OdpsException, ODPSConsoleException {
    int retryTimes = 0;

    XFlows xFlows = odps.xFlows();
    int interval = progressHelper.getInterval();

    boolean terminatedFlag = false;
    while (true) {
      try {
        for (XFlows.XResult xResult : xFlows.getXResults(xInstance).values()) {
          if ("SubWorkflow".equalsIgnoreCase(xResult.getNodeType())) {
            waitForInstanceTerminate(progressHelper, logviewHasPrintSet, odps.instances().get(xResult.getInstanceId()), odps, context);
          } else if (!"Local".equalsIgnoreCase(xResult.getNodeType())) {
            Instance i = odps.instances().get(odps.getDefaultProject(), xResult.getInstanceId());

            String ID = i.getId();

            if (!logviewHasPrintSet.contains(ID)) {
              logviewHasPrintSet.add(ID);
              System.err.println("Sub Instance ID = " + ID);
              System.err.println(ODPSConsoleUtils.generateLogView(odps, i, context));
            }

            if (progressHelper.needProgressMessage(i)) {
              System.err.println(progressHelper.getProgressMessage(i));
            }
          }
        }

        //FOR CNN ALGO, we should print one more progress message after terminated.
        if  (xInstance.isTerminated()) {
          if (terminatedFlag) {
            break;
          } else {
            terminatedFlag = true;
          }
        }

        Thread.sleep(interval);
        retryTimes = 0;
      } catch (InterruptedException e) {
        throw new UserInterruptException(e.getMessage());
      } catch (OdpsException e) {
        // retry for service will throw exception in 400 code immediately
        // so need retry and add interval time.
        ++retryTimes;
        try {
          Thread.sleep(2 * interval);
        } catch (InterruptedException e1) {
          throw new UserInterruptException(e.getMessage());
        }
        if (retryTimes > MAX_RETRY_TIMES) {
          throw e;
        }
        System.err.println(String.format("retry %d times.", retryTimes));
      } catch (ReloadException e) {
        ++retryTimes;
        try {
          Thread.sleep(2 * interval);
        } catch (InterruptedException e1) {
          throw new UserInterruptException(e.getMessage());
        }
        if (retryTimes > MAX_RETRY_TIMES) {
          throw e;
        }
        System.err.println(String.format("retry %d times.", retryTimes));
      }
    }
  }

  public static void waitForCompletion(Instance xInstance, Odps odps,
                                       ExecutionContext context,
                                       String url)
      throws OdpsException, ODPSConsoleException {
    XFlows xFlows = odps.xFlows();

    String algoName = xFlows.getXFlowInstance(xInstance).getXflowName();

    XFlowProgressHelper progressHelper = XFlowProgressHelperRegistry.getProgressHelper(
        algoName.toUpperCase());

    if (progressHelper == null) {
      progressHelper = new XFlowStageProgressHelper();
    }
    progressHelper.setConfig(url);

    Set<String> logviewHasPrintSet = new HashSet<String>();

    try {
      waitForInstanceTerminate(progressHelper, logviewHasPrintSet, xInstance, odps, context);
    } catch (UserInterruptException e) {
      context.getOutputWriter().writeError("Instance running background.");
      context.getOutputWriter().writeError(
          "Use \'kill " + xInstance.getId() + "\' to stop this instance.");
      context.getOutputWriter().writeError(
          "Use \'wait " + xInstance.getId() + "\' to get details of this instance.");
      throw e;
    }

    if (xInstance.isSuccessful()) {
      if (printResultList.contains(algoName.toUpperCase())) {
        for (XResult result : xFlows.getXResults(xInstance).values()) {
          if (!StringUtils.isNullOrEmpty(result.getResult())) {
            context.getOutputWriter()
                .writeResult(result.getInstanceId() + ":" + result.getResult());
          }
        }
      }
      System.err.println("OK");
      return;
    }

    checkFailedXInstance(odps, xInstance);
  }

  private static void checkFailedXInstance(Odps odps, Instance xInstance)
      throws OdpsException, ODPSConsoleException {
    System.err.println("Instance " + xInstance.getId() + " Failed.");

    for (XResult result : odps.xFlows().getXResults(xInstance).values()) {
      if ("SubWorkflow".equalsIgnoreCase(result.getNodeType())) {
        checkFailedXInstance(odps, odps.instances().get(result.getInstanceId()));
      } else if (!"Local".equalsIgnoreCase(result.getNodeType())) {
        Instance i = odps.instances().get(result.getInstanceId());
        if (!i.isSuccessful()) {
          throw new OdpsException("Failed " + i.getId() + ":" + result.getResult());
        }
      }
    }

    for (Entry<String, TaskStatus> task : xInstance.getTaskStatus().entrySet()) {
      if (task.getValue().getStatus() != TaskStatus.Status.SUCCESS) {
        String result = xInstance.getTaskResults().get(task.getKey());
        throw new OdpsException("Failed Task " + task.getKey() + ":" + result);
      }
    }
    throw new OdpsException("Instance " + xInstance.getId() + " Failed");
  }

  private static void replaceProperty(Properties properties, String project) {
    String inputTableName = properties.getProperty("inputTableName");
    if (!StringUtils.isNullOrEmpty(inputTableName)) {
      if (!inputTableName.contains(".")) {
        inputTableName = project + "." + inputTableName;
        properties.setProperty("inputTableName", inputTableName);
      }
    }

    String outputTableName = properties.getProperty("outputTableName");
    if (!StringUtils.isNullOrEmpty(outputTableName)) {
      if (!outputTableName.contains(".")) {
        outputTableName = project + "." + outputTableName;
        properties.setProperty("outputTableName", outputTableName);
      }
    }
    String modelName = properties.getProperty("modelName");
    if (!StringUtils.isNullOrEmpty(modelName)) {
      if (!modelName.contains("/")) {
        modelName = project + "/offlinemodels/" + modelName;
        properties.setProperty("modelName", modelName);
      }
    }
  }

  public final static Pattern regex = Pattern.compile("\\s*PAI($|\\s.*)", Pattern.DOTALL
                                                                    | Pattern.CASE_INSENSITIVE);

  public static PAICommand parse(String commandString, ExecutionContext sessionContext)
      throws ODPSConsoleException {

    // 检查是否符合 PAI 命令
    if (regex.matcher(commandString).matches()) {
      return new PAICommand(commandString, sessionContext);
    }
    return null;
  }
}
