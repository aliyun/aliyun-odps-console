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

import java.lang.reflect.Method;

import org.junit.Test;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskSummary;
import com.aliyun.odps.Odps;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.output.InstanceRunner;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class InstanceRunnerTest {

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
    Method method = runner.getClass().getDeclaredMethod("getTaskSummaryV1", Instance.class, String.class);
    method.setAccessible(true);
    Instance instance = runner.getInstance();
    System.out.println(instance);
    assertNotNull(instance);
    TaskSummary summary = (TaskSummary) method.invoke(runner, instance, "sqltest");
    String summaryText = summary.getSummaryText();
    System.out.println(summaryText);
    assertNotNull(summaryText);
    assertTrue(summaryText.startsWith("resource cost"));
  }
}
