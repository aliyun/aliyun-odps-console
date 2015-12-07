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

/**
 * 
 */
package com.aliyun.openservices.odps.console.output;

import java.util.List;
import java.util.Map;

import com.aliyun.odps.Instance.StageProgress;

/**
 * @author xiaoming.yin
 * 
 */
public interface ProgressListener {

  /**
   * 返回要监听进度的任务名称。
   * 
   * @return 要监听进度的任务名称。
   */
  Iterable<String> getTaskNames();

  /**
   * 处理进度。
   * 
   * @param progresses
   *          监听的任务进度。
   */
  void report(Map<String, List<StageProgress>> progresses);
}
