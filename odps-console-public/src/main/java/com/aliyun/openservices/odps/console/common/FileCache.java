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

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instance.TaskStatus;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

import java.io.*;
import java.nio.channels.FileLock;
import java.util.Map;

public class FileCache {

  public FileCache(int capacity) {
    cacheDir = getLocalAppDir();
    this.capacity = capacity * 1024 * 1024;
  }

  private File cacheDir;
  private int capacity;

  @SuppressWarnings("resource")
  public InputStream get(Instance inst, String taskName) throws ODPSConsoleException {

    try {
      // Do not cache running instance, since it's details is dynamic
      Map<String, TaskStatus> taskStatux = inst.getTaskStatus();
      TaskStatus taskStatus = taskStatux.get(taskName);
      if (taskStatus == null) {
        throw new ODPSConsoleException("Instance '" + inst.getId() + "' has no such task: " + taskName);
      }

      boolean needLock = !(taskStatus.getStatus().equals(TaskStatus.Status.RUNNING) || taskStatus
          .getStatus().equals(TaskStatus.Status.WAITING));
      if (!needLock) {
        return fireOnNet(inst, taskName);
      }
    } catch (OdpsException e2) {
      throw new ODPSConsoleException(e2);
    }

    String key = inst.getProject() + "-" + inst.getId() + "-" + taskName;
    File file = new File(cacheDir, key);
    File lockFile = new File(cacheDir, ".lock");
    RandomAccessFile locFile = null;
    FileLock lock = null;
    try {
      lockFile.createNewFile();
      locFile = new RandomAccessFile(lockFile, "rw");
      lock = locFile.getChannel().lock();
    } catch (IOException e) {
    }

    if (lock == null) {
      // Failed to obtain lock, request directly online
      return fireOnNet(inst, taskName);
    }

    try {
      InputStream in = new FileInputStream(file);
      return in;
    } catch (FileNotFoundException e) {
      try {
        InputStream in = fireOnNet(inst, taskName);
        int contentSize = getContentSize();
        if (contentSize > capacity) {
          clearCache();
          contentSize = 0;
        }
        file.createNewFile();
        FileOutputStream out = new FileOutputStream(file);
        byte buf[] = new byte[1024];
        int len = -1;
        int aggrSize = 0;
        while ((len = in.read(buf)) != -1) {
          aggrSize += len;
          out.write(buf, 0, len);
        }
        contentSize += aggrSize;
        out.close();
        return new FileInputStream(file);
      } catch (IOException e1) {
        throw new ODPSConsoleException(e1);
      }
    } finally {
      try {
        lock.release();
      } catch (IOException e) {
        throw new ODPSConsoleException(e);
      }
    }
  }

  private File getLocalAppDir() {
    File d = new File(getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
    File dir = d.getParentFile().getParentFile();
    File cacheDirFile = new File(dir, ".CacheFiles");
    cacheDirFile.mkdir();
    return cacheDirFile;
  }

  private int getContentSize() {
    File[] files = cacheDir.listFiles();
    long len = 0;
    for (File f : files) {
      if (f.isFile()) {
        len += f.length();
      }
    }
    return (int) len;
  }

  private void clearCache() {
    for (File f : cacheDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(File arg0) {
        return arg0.getName().charAt(0) != '.';
      }

    })) {
      if (f.isFile()) {
        f.delete();
      }
    }
  }

  private InputStream fireOnNet(Instance inst, String taskName) throws ODPSConsoleException {
    try {
      return new ByteArrayInputStream(inst.getTaskDetailJson2(taskName).getBytes());
    } catch (OdpsException e) {
      throw new ODPSConsoleException(e);
    }
  }
}
