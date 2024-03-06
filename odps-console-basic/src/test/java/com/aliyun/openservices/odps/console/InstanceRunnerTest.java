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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.InstanceFilter;
import com.aliyun.odps.Instances;
import com.aliyun.odps.Job;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.ReloadException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.output.InstanceRunner;
import com.aliyun.openservices.odps.console.output.state.InstanceRunningTest;
import com.aliyun.openservices.odps.console.output.state.InstanceSuccess;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class InstanceRunnerTest {

  static boolean listCall = false;
  static int createCount = 0;
  static int statusCount = 0;
  static int progressCount = 0;
  static Instance.StageProgress stageProgress;

  static {
    stageProgress = Mockito.spy(new Instance.StageProgress());

    when(stageProgress.getBackupWorkers()).thenReturn(0);
    when(stageProgress.getFinishedPercentage()).thenReturn(2);
    when(stageProgress.getStatus()).thenReturn(Instance.StageProgress.Status.RUNNING);
    when(stageProgress.getName()).thenReturn("test_stage_2");
    when(stageProgress.getRunningWorkers()).thenReturn(1);
    when(stageProgress.getTerminatedWorkers()).thenReturn(1);
    when(stageProgress.getTotalWorkers()).thenReturn(2);
  }

  @Test
  public void testGetTaskSummary() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from src;");
    task.setName("sqltest");
    InstanceRunner runner = new InstanceRunner(odps, task,  context);
    runner.submit();
    runner.waitForCompletion();
    Method method = InstanceSuccess.class.getDeclaredMethod("getTaskSummaryV1", Odps.class,
                                                            Instance.class, String.class);
    method.setAccessible(true);
    Instance instance = runner.getInstance();
    System.out.println(instance);
    assertNotNull(instance);
    TaskSummary summary = (TaskSummary) method.invoke(new InstanceSuccess(), odps, instance, "sqltest");
    String summaryText = summary.getSummaryText();
    System.out.println(summaryText);
    assertNotNull(summaryText);
    assertTrue(summaryText.contains("resource cost"));
  }

  @Test(expected = OdpsException.class)
  public void testSubmitRetry_failed() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = Mockito.spy(OdpsConnectionFactory.createOdps(context));
    Instances instances = Mockito.spy(odps.instances());
    when(odps.instances()).thenReturn(instances);

    doReturn(odps).when(odps).clone();

    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from src;");
    task.setName("sqltest");
    InstanceRunner runner = new InstanceRunner(odps, task,  context);

    createCount = 0;
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        createCount ++;
          throw new OdpsException("", new IOException());}
    }).when(instances).create(any(Job.class));

    listCall = false;
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        listCall = true;
        return invocation.callRealMethod();
      }
    }).when(instances).iterator(any(InstanceFilter.class));

    try {
      runner.submit();
    } catch (OdpsException e) {
      Assert.assertNull(runner.getInstance());
      Assert.assertTrue(listCall);
      Assert.assertEquals(2, createCount);
      Assert.assertTrue(e.getMessage().startsWith("无法获取ID"));

      throw e;
    }

    Assert.fail();
  }


  @Test
  public void testPollingOutput() throws Exception {
    PrintStream io = new PrintStream("out.txt");

    System.setErr(io);
    System.setOut(System.err);

    ExecutionContext context = ExecutionContext.init();
    Odps odps = Mockito.spy(OdpsConnectionFactory.createOdps(context));
    Instances instances = Mockito.spy(odps.instances());

    SQLTask task = new SQLTask();
    final String taskname = "sqltest";
    task.setQuery("select count(*)+1 from src;");
    task.setName(taskname);

    Instance instance = Mockito.spy(instances.create(task));

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        statusCount ++;

        if (statusCount == 3) {
          throw new ReloadException("test");
        }

        if (statusCount > 8) {
          return Instance.Status.TERMINATED;
        }

        return Instance.Status.RUNNING;
      }
    }).when(instance).getStatus(true);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (progressCount++ == 0) {
          return null;
        }

        if (progressCount == 1) {
          throw new OdpsException("get progress failed.");
        }

        if (progressCount == 2) {
          List<Instance.StageProgress> res = new ArrayList<Instance.StageProgress>();
          return res;
        }

        if (progressCount == 3) {
          List<Instance.StageProgress> res = new ArrayList<Instance.StageProgress>();
          res.add(InstanceRunningTest.progress);
          res.add(stageProgress);
          return res;
        }

        if (progressCount == 5) {
          List<Instance.StageProgress> res = new ArrayList<Instance.StageProgress>();
          res.add(stageProgress);
          return res;
        }

        return null;
      }
    }).when(instance).getTaskProgress(taskname);


    Map<String, Instance.TaskStatus> taskStatuses = new HashMap<String, Instance.TaskStatus>();
    Instance.TaskStatus status = Mockito.mock(Instance.TaskStatus.class);
    when(status.getStatus()).thenReturn(Instance.TaskStatus.Status.SUCCESS);
    when(status.getName()).thenReturn(taskname);
    taskStatuses.put(taskname, status);

    doReturn(taskStatuses).when(instance).getTaskStatus();

    Map<String, String> result = new HashMap<String, String>();
    result.put(taskname, "OK");
    doReturn(result).when(instance).getTaskResults();

    String warnings = "{\"warnings\":[\"warnings 1.\",\"warnings 2.\"]}";
    doReturn(warnings).when(instance).getTaskInfo(taskname, "warnings");

    InstanceRunner runner = Mockito.spy(new InstanceRunner(odps, instance, context));
    runner.waitForCompletion();
    System.err.flush();

    Iterator<String> taskResult = runner.getResult();
    while (taskResult.hasNext()) {
      context.getOutputWriter().writeResult(taskResult.next());
    }
  }


  @Test
  public void testSubmitRetry_listFailed() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = Mockito.spy(OdpsConnectionFactory.createOdps(context));
    Instances instances = Mockito.spy(odps.instances());
    when(odps.instances()).thenReturn(instances);

    doReturn(odps).when(odps).clone();

    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from src;");
    task.setName("sqltest");
    InstanceRunner runner = new InstanceRunner(odps, task,  context);

    createCount = 0;

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        createCount ++;
        if (createCount == 1) {
          throw new OdpsException("", new IOException());
        }
        if (createCount == 2) {
          return invocation.callRealMethod();
        }

        return null;
      }
    }).when(instances).create(any(Job.class));

    listCall = false;
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        listCall = true;
        return invocation.callRealMethod();
      }
    }).when(instances).iterator(any(InstanceFilter.class));

    runner.submit();
    Assert.assertNotNull(runner.getInstance());
    // confirm the instance, get it when list on retry
    Assert.assertTrue(listCall);
    Assert.assertEquals(2, createCount);
    Assert.assertNotNull(runner.getInstance().getOdpsHooks());
    runner.waitForCompletion();
  }

  @Test
  public void testSubmitRetry_listSuccess() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = Mockito.spy(OdpsConnectionFactory.createOdps(context));
    Instances instances = Mockito.spy(odps.instances());
    when(odps.instances()).thenReturn(instances);

    doReturn(odps).when(odps).clone();

    SQLTask task = new SQLTask();
    task.setQuery("select count(*) from src;");
    task.setName("sqltest");
    InstanceRunner runner = new InstanceRunner(odps, task,  context);

    createCount = 0;
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        createCount ++;
        invocation.callRealMethod();
        throw new OdpsException("", new IOException());
      }
    }).when(instances).create(any(Job.class));

    listCall = false;
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        listCall = true;
        return invocation.callRealMethod();
      }
    }).when(instances).iterator(any(InstanceFilter.class));

    runner.submit();
    Assert.assertNotNull(runner.getInstance());
    // confirm the instance, get it when list on retry
    Assert.assertTrue(listCall);
    Assert.assertEquals(1, createCount);
    Assert.assertNotNull(runner.getInstance().getOdpsHooks());
    runner.waitForCompletion();
  }


}
