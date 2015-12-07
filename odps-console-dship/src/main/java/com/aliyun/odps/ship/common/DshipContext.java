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

package com.aliyun.odps.ship.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.aliyun.openservices.odps.console.ExecutionContext;

/**
 * Created by lulu on 15-2-9.
 */
public enum DshipContext {
  INSTANCE;
  private Map<String, String> context = new HashMap<String, String>();

  public ExecutionContext getExecutionContext() {
    return executionContext;
  }

  public void setExecutionContext(ExecutionContext executionContext) {
    this.executionContext = executionContext;
  }

  private ExecutionContext executionContext;

  public void put(String key, String value) {
    context.put(key, value);
  }

  public String get(String key) {
    if (context.containsKey(key)) {
      return context.get(key);
    }
    return null;
  }

  public Set<String> keySet() {
    return context.keySet();
  }

  public void clear() {
    context.clear();
  }

  public int size() {
    return context.size();
  }

  public boolean containsKey(String key) {
    return context.containsKey(key);
  }

  public void remove(String key) {
    context.remove(key);
  }

  public Map<String, String> getAll() {
    return context;
  }

  public void putAll(Map<String, String> ctx) {
    context.putAll(ctx);
  }

}
