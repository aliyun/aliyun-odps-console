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

package com.aliyun.openservices.odps.console.common;

import java.io.InputStream;
import java.util.TreeMap;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Project;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class InstanceContext {
  private Odps odps;

  private ExecutionContext session;
  private String errorMsg;
  private Project project;
  private Instance instance;
  private String task;

  private TreeMap<String, String> logDir = new TreeMap<String, String>();

  private static final int MAX_FILES = 42;
  private static final FileCache fileCache = new FileCache(MAX_FILES);

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String msg) {
    errorMsg = msg;
  }

  public Project getProject() {
    return project;
  }

  public void setProject(Project project) {
    this.project = project;
  }

  public void setProjectByName(String projName) throws OdpsException {
    if (project == null || !project.getName().toLowerCase().equals(projName.toLowerCase())) {
      project = odps.projects().get(projName);
      // Project has been changed, so instance should also be cleared
      // in this case.
      reset(null);
    }
  }

  public Instance getInstance() {
    return instance;
  }

  public void setInstance(Instance instance) {
    reset(instance);
  }

  public void setInstanceById(String id) {
    id = id.toLowerCase();
    if (instance == null || !id.equals(instance.getId())) {
      reset(odps.instances().get(id));
    }
  }

  public void reset(Instance instance) {
    if (instance != this.instance) {
      // Clear instance related status
      logDir.clear();
      task = null;
    }
    this.instance = instance;
  }

  public ExecutionContext getSession() {
    return session;
  }

  public void setSession(ExecutionContext execCtx) {
    this.session = execCtx;
  }

  public Odps getConn() {
    return odps;
  }

  public void setConn(Odps odps) {
    this.odps = odps;
  }

  public TreeMap<String, String> getLogDir() {
    return logDir;
  }

  public void setLogDir(TreeMap<String, String> logDir) {
    this.logDir = logDir;
  }

  public String getTask() {
    return task;
  }

  public void setTask(String task) {
    this.task = task;
  }

  public InputStream getTaskDetails(Instance inst, String taskName) throws ODPSConsoleException {
    return fileCache.get(inst, taskName);
  }
}
