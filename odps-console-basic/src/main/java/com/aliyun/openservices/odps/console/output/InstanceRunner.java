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

package com.aliyun.openservices.odps.console.output;

import static com.aliyun.openservices.odps.console.utils.CodingUtils.assertParameterNotNull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.map.ObjectMapper;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.StageProgress;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.InstanceFilter;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.odps.commons.util.JacksonParser;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.output.InstanceProgress.InstanceStage;

import jline.console.UserInterruptException;

/**
 * @author shuman.gansm 执行odps task的帮助类
 * */
public class InstanceRunner {

  private ExecutionContext context;
  private Odps odps;

  private List<Task> tasks;
  private IProgressReporter reporter;
  private Options options;
  private Instance instance;

  // 如果异常启动，用户可以随时获取instance状态
  InstanceProgress progress;

  Map<String, Integer> readyCounts = new HashMap<String, Integer>();
  Long baseTime = new Date().getTime();

  public Instance getInstance() {
    return instance;
  }

  public InstanceProgress getProgress() {
    return progress;
  }

  public InstanceRunner(Odps odps, Task task, ExecutionContext context) {
    this(odps, Arrays.asList(task), context);
  }

  public InstanceRunner(Odps odps, Instance instance, ExecutionContext context)
      throws OdpsException {
    this(odps, instance, instance.getTasks(), context);
  }

  public InstanceRunner(Odps odps, List<Task> tasks, ExecutionContext context) {
    this(odps, null, tasks, context);
  }

  private InstanceRunner(Odps odps, Instance instance, List<Task> tasks, ExecutionContext context) {
    assertParameterNotNull(odps, "odps");

    if (tasks == null || tasks.size() == 0) {
      throw new IllegalArgumentException("tasks");
    }

    this.instance = instance;

    this.odps = odps;
    this.tasks = tasks;
    this.reporter = new InstanceProgressReporter(context, odps);
    // 默认重试50次, 每次sleep 5秒
    this.options = new Options(50, 5, context.getPriority());
    this.context = context;
  }
  /**
   * 异步方式
   * 
   * @return 返回生成的JobInstance
   * */
  public Instance submit() throws OdpsException {

    instance = submitWithRetry();

    return instance;
  }

  private Instance submitWithRetry() throws OdpsException {
    Job job = new Job();
    String guid = UUID.randomUUID().toString();
    for (Task task : tasks) {
      task.setProperty("guid", guid);
      job.addTask(task);
    }

    job.setPriority(options.getPriority());
    job.setRunningCluster(context.getRunningCluster());

    try {
      instance = odps.instances().create(job);
      return instance;
    } catch (OdpsException e) {
      if (!isRetry(e)) {
        throw e;
      }
    }

    try {
      instance = retry(job, guid);
    } catch (Exception e1) {
      throw new OdpsException("无法获取ID，请稍后重试。[" + e1.getMessage() + "]", e1);
    }
    return instance;
  }

  private boolean isRetry(OdpsException e) {

    if (e.getCause() != null && e.getCause() instanceof IOException) {
      // FOR NormalException, MUST RETRY HERE
      return true;
    }

    String[] errorCodes = { "ODPS-0430055", "ODPS_0410077", "ODPS_0410065" };
    String errorMsg = e.getMessage();

    for (String ec : errorCodes) {
      // if response message inculde error code
      if (errorMsg.indexOf(ec) >= 0) {
        return true;
      }
    }

    return false;
  }

  private Instance retry(Job job, String guid) throws Exception {
    Odps odps = this.odps.clone();
    odps.getRestClient().setRetryTimes(0);
    Response response = odps.getRestClient().request("/projects/" + odps.getDefaultProject(),
                                                     "GET", null, null, null);
    Date sDate = DateUtils.parseRfc822Date(response.getHeader("Date"));
    final Date fromDate = new Date(sDate.getTime() - 300 * 1000);
    final Date toDate = new Date(sDate.getTime() + 10 * 1000);
    InstanceFilter filter = new InstanceFilter();
    filter.setFromTime(fromDate);
    filter.setEndTime(toDate);
    Iterator<Instance> iterator = odps.instances().iterator(filter);
    while (iterator.hasNext()) {
      Instance instance = iterator.next();
      for (Task task : instance.getTasks()) {
        if (guid.equals(task.getProperties().get("guid"))) {
          return instance;
        }
      }
    }
    return odps.instances().create(job);
  }

  /**
   * 同步方式
   * 
   * @return 返回task的执行结果
   * **/
  public String waitForCompletion() throws OdpsException {

    // 生成instance id并且等待instance执行成功
    if (instance == null) {
      // 如果没有instance，先提交instance
      submit();
    }

    progress = new InstanceProgress(InstanceStage.CREATE_INSTANCE, instance, tasks.get(0));
    reporter.report(progress);

    // 等待instance执行结束
    try {
      checkInstanceStatus();
    } catch (UserInterruptException e) {
      System.err.println("Instance running background.");
      System.err.println("Use \'kill " + instance.getId() + "\' to stop this instance.");
      throw e;
    }

    Task currentTask = getCurrentTask();
    // 获取task的执行状态
    TaskStatus taskStatus = getTaskStatus(currentTask);
    // 获取task的执行结果
    String result = getResult(currentTask);

    // 获取TaskSummary信息
    TaskSummary taskSummary = null;
    try {
      taskSummary = getTaskSummaryV1(instance, currentTask.getName());
    } catch (Exception e) {
    }

    progress = new InstanceProgress(InstanceStage.FINISH, instance, currentTask);
    progress.setTaskStatus(taskStatus);
    progress.setSummary(taskSummary);
    progress.setResult(result);
    reporter.report(progress);

    if (TaskStatus.Status.FAILED.equals(taskStatus.getStatus())) {
      if (dealWithMultiCluster(currentTask, result)) {
        // 重新拿一次result，返回给用户
        result = getResult(currentTask);
      } else {
        // 如果不是跨集群，直接抛出异常
        throw new OdpsException(result);
      }
    } else if (TaskStatus.Status.CANCELLED.equals(taskStatus.getStatus())) {
      throw new OdpsException("Task got cancelled");
    }

    return result;
  }

  // XXX very dirty !!!
  // DO HACK HERE
  private TaskSummary getTaskSummaryV1(Instance i, String taskName) throws Exception {
    RestClient client = odps.getRestClient();
    Map<String, String> params = new HashMap<String, String>();
    params.put("summary", null);
    params.put("taskname", taskName);
    String queryString = "/projects/" + i.getProject() + "/instances/" + i.getId();
    Response result = client.request(queryString, "GET", params, null, null);

    TaskSummary summary = null;
    ObjectMapper objMapper = JacksonParser.getObjectMapper();
    Map<Object, Object> map = objMapper.readValue(result.getBody(), Map.class);
    if (map.get("mapReduce") != null) {
      Map mapReduce = (Map) map.get("mapReduce");
      String jsonSummary = (String) mapReduce.get("jsonSummary");
      summary = new TaskSummary();
      if (jsonSummary == null) {
        jsonSummary = "{}";
      }
      if (summary != null) {
        Field textFiled = summary.getClass().getDeclaredField("text");
        textFiled.setAccessible(true);
        textFiled.set(summary, (String) mapReduce.get("summary"));
        Field jsonField = summary.getClass().getDeclaredField("jsonSummary");
        jsonField.setAccessible(true);
        jsonField.set(summary, jsonSummary);
      }
    }
    return summary;
  }

  /**
   * 执行instance
   * */
  private void checkInstanceStatus() throws OdpsException {

    WaitSettings setting = new WaitSettings();
    setting.setContinueOnError(true);
    setting.setMaxErrors(options.getMaxErrors());
    setting.setPauseStrategy(new Runnable() {
      public void run() {
        try {
          TimeUnit.SECONDS.sleep(options.getSleepTime());
        } catch (InterruptedException e) {
          throw new UserInterruptException("In checkInstanceStatus(), InterruptedException occur.");
        }
      }
    });

    DefaultProgressListener listener = new DefaultProgressListener(progress, reporter, tasks);
    waitForCompletion(instance, setting, listener);

  }

  public void waitForCompletion(Instance instance, WaitSettings settings, ProgressListener listener)
      throws OdpsException {
    assertParameterNotNull(settings, "settings");

    boolean listenProgress = listener != null;
    long startTime = new Date().getTime();

    // 连续出错才失败
    int errors = 0;
    while (true) {
      try {

        if (instance.getStatus() == Instance.Status.TERMINATED) {
          break;
        }

        // Report progress
        if (listenProgress) {

          try {
            Map<String, List<StageProgress>> progresses = new HashMap<String, List<StageProgress>>();
            for (String taskName : listener.getTaskNames()) {
              List<StageProgress> progress = instance.getTaskProgress(taskName);
              progresses.put(taskName, progress);
            }
            listener.report(progresses);
          } catch (Exception e) {
            // 如果拿进度出错，不抛出来
          }
        }

        // Pause to next retry
        settings.pauseStrategy.run();

        // Check timeout
        if (settings.timeout > 0 && (new Date().getTime() - startTime >= settings.timeout))
          throw new OdpsException("等待超时。");

        // 如果执行到这里，则本次执行成功,error 重新清零
        errors = 0;

      } catch (OdpsException ex) {
        if (!shouldRetryWait(settings, ++errors, startTime)) {
          throw ex;
        }

        // if got error, pause
        settings.pauseStrategy.run();
      }
    }
  }

  private boolean shouldRetryWait(WaitSettings settings, int errors, long startTime) {
    if (!settings.continueOnError) {
      return false;
    }

    if (errors >= settings.maxErrors) {
      return false;
    }

    if (settings.timeout > 0 && (new Date().getTime() - startTime >= settings.timeout)) {
      return false;
    }

    return true;
  }

  /**
   * 取task的返回结果
   * */
  private String getResult(final Task task) throws OdpsException {
    Map<String, String> resultMap = instance.getTaskResults();
    return resultMap.get(task.getName());
  }

  /**
   * 取task的执行状态
   * **/
  private TaskStatus getTaskStatus(final Task task) throws OdpsException {

    TaskStatus taskStatus = instance.getTaskStatus().get(task.getName());
    if (taskStatus == null) {
      // 如果task还没有进行，被kill掉了会出异常
      throw new OdpsException("can not get task status.");
    }

    return taskStatus;
  }

  private boolean dealWithMultiCluster(Task task, String rawResult) throws OdpsException {
    return false;
  }

  private Task getTask(String taskName) {

    for (Task task : tasks) {
      if (task.getName().equalsIgnoreCase(taskName)) {
        return task;
      }
    }
    return null;
  }

  /**
   * 当前task指: 第一个task fail的task，如果没有fail的，指最后一个找到running的task(因为有并行的instance)
   * */
  private Task getCurrentTask() throws OdpsException {

    if (instance == null) {
      throw new OdpsException("can not find instance.");
    }

    // 当前task名称
    String cTaskName = null;
    Map<String, TaskStatus> taskStatuses = instance.getTaskStatus();
    for (String taskName : taskStatuses.keySet()) {
      TaskStatus tStatus = taskStatuses.get(taskName);
      if (TaskStatus.Status.FAILED.equals(tStatus.getStatus())) {
        cTaskName = taskName;
      } else if (TaskStatus.Status.RUNNING.equals(tStatus.getStatus())) {
        cTaskName = taskName;
      }
    }

    if (cTaskName == null) {
      cTaskName = tasks.get(tasks.size() - 1).getName();

      TaskStatus status = taskStatuses.get(cTaskName);
      if (status == null) {
        throw new OdpsException("task status unknown. taskname=" + cTaskName);
      } else if (!TaskStatus.Status.SUCCESS.equals(status.getStatus())) {
        throw new OdpsException("task status: " + status.getStatus());
      }
    }

    return getTask(cTaskName);
  }

}
