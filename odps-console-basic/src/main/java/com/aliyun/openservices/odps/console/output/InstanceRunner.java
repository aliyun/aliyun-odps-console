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
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.aliyun.odps.Instance;
import com.aliyun.odps.InstanceFilter;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsHooks;
import com.aliyun.odps.Task;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.commons.util.DateUtils;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.output.state.InstanceCreated;
import com.aliyun.openservices.odps.console.output.state.InstanceStateContext;
import com.aliyun.openservices.odps.console.output.state.InstanceTerminated;
import com.aliyun.openservices.odps.console.utils.statemachine.DefaultStateManager;

import jline.console.UserInterruptException;

/**
 * @author shuman.gansm 执行odps task的帮助类
 */
public class InstanceRunner {
  private ExecutionContext context;
  private Odps odps;

  private List<Task> tasks;
  private String result;
  private Options options;
  private Instance instance;
  private InstanceStateContext instanceStateContext;

  // 如果异常启动，用户可以随时获取instance状态

  public Instance getInstance() {
    return instance;
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
    // 默认重试50次, 每次sleep 5秒
    this.options = new Options(50, 5, context.getPriority());
    this.context = context;
  }


  /**
   * 异步方式
   *
   * @return 返回生成的JobInstance
   */
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

    String[] errorCodes = {"ODPS-0430055", "ODPS_0410077", "ODPS_0410065"};
    String errorMsg = e.getMessage();

    for (String ec : errorCodes) {
      // if response message inculde error code
      if (errorMsg != null && errorMsg.indexOf(ec) >= 0) {
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

          if (OdpsHooks.isEnabled()) {
            OdpsHooks hooks = new OdpsHooks();
            // set hooks to this instance
            instance.setOdpsHooks(hooks);
            // create instance failed and hooks had not been invoked before retry
            // invoke instance on created hooks here
            hooks.onInstanceCreated(instance, this.odps);
          }

          return instance;
        }
      }
    }
    return odps.instances().create(job);
  }

  /**
   * 同步方式
   *
   * 返回task的执行结果 {@link #getResult()}
   **/
  public void waitForCompletion() throws OdpsException {

    // 生成instance id并且等待instance执行成功
    if (instance == null) {
      // 如果没有instance，先提交instance
      submit();
    }

    instanceStateContext = new InstanceStateContext(odps, instance, context);
    DefaultStateManager instanceStateManager = new DefaultStateManager<InstanceStateContext>();

    if (instance.isSync()) {
      // 同步任务也输出 instance id 方便定位问题
      instanceStateContext.printInstanceId();
      instance.getStatus(); // 触发 after hook
      // 同步任务不需要后续的 progress/summary 等请求，
      // 直接跳转 InstanceTerminated 开始
      instanceStateManager.start(instanceStateContext, new InstanceTerminated());
    } else {
      try {
        // 开始启动作业等待状态机，从 InstanceCreated 开始
        instanceStateManager.start(instanceStateContext, new InstanceCreated());
      } catch (UserInterruptException e) {

        context.getOutputWriter().writeError("Instance running background.");
        context.getOutputWriter().writeError(
            "Use \'kill " + instance.getId() + "\' to stop this instance.");
        context.getOutputWriter().writeError(
            "Use \'wait " + instance.getId() + "\' to get details of this instance.");
        throw e;
      }
    }
  }

  public void printLogview() throws OdpsException {
    instanceStateContext = new InstanceStateContext(odps, instance, context);
    instanceStateContext.printInstanceId();
    instanceStateContext.printLogview();
  }

  public InstanceStateContext getInstanceStateContext() throws OdpsException {
    return new InstanceStateContext(odps, instance, context);
  }

  public String getResult() throws OdpsException {
    if (instanceStateContext != null) {
      // should invoke waitForCompletion first
      return instanceStateContext.getResult();
    } else {
      return null;
    }
  }
}
