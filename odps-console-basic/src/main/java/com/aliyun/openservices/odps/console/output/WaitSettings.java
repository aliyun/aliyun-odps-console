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

import jline.console.UserInterruptException;

/**
 * 等待作业运行实例执行结束的设置。
 * 
 * @author xiaoming.yin
 * 
 */
public class WaitSettings {
  int timeout = 0;
  boolean continueOnError = true;
  int maxErrors = 50;
  Runnable pauseStrategy = new Runnable() {
    @Override
    public void run() {
      final int interval = 5 * 1000; // ms
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        throw new UserInterruptException("interrupted while thread sleep");
        // Just return if the thread is interrupted
      }
    }
  };

  /**
   * 构造函数。
   */
  public WaitSettings() {
  }

  /**
   * 返回等待的超时时间。
   * 
   * @return 等待的超时时间。
   */
  public int getTimeout() {
    return timeout;
  }

  /**
   * 设置等待的超时时间。
   * 
   * @param timeout
   *          等待的超时时间。
   */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /**
   * 返回一个值表示当获取状态失败时是否继续等待。
   * 
   * @return 当获取状态失败时是否继续等待。
   */
  public boolean isContinueOnError() {
    return continueOnError;
  }

  /**
   * 设置一个值表示当获取状态失败时是否继续等待。
   * 
   * @param continueOnError
   *          当获取状态失败时是否继续等待。
   */
  public void setContinueOnError(boolean continueOnError) {
    this.continueOnError = continueOnError;
  }

  /**
   * 返回一个值表示获取状态和进度信息时最多的失败次数。
   * 
   * @return 获取状态和进度信息时最多的失败次数。
   */
  public int getMaxErrors() {
    return maxErrors;
  }

  /**
   * 设置一个值表示获取状态和进度信息时最多的失败次数。
   * 
   * @param maxRetries
   *          获取状态和进度信息时最多的失败次数。
   */
  public void setMaxErrors(int maxRetries) {
    this.maxErrors = maxRetries;
  }

  /**
   * 返回一个实现{@link Runnable}接口的对象，表示每次轮询之间的暂停策略。
   * <p>
   * 默认的暂停策略是再次询问状态之间线程睡眠1秒钟，即每隔1秒钟查询一次状态。
   * </p>
   * 
   * @return 每次轮询之间的暂停策略。
   */
  public Runnable getPauseStrategy() {
    return pauseStrategy;
  }

  /**
   * 设置一个实现{@link Runnable}接口的对象，表示每次轮询之间的暂停策略。
   * <p>
   * 默认的暂停策略是再次询问状态之间线程睡眠1秒钟，即每隔1秒钟查询一次状态。 设置时不允许传入null。
   * </p>
   * 
   * @param pauseStrategy
   *          每次轮询之间的暂停策略。
   */
  public void setPauseStrategy(Runnable pauseStrategy) {
    assertParameterNotNull(pauseStrategy, "pauseStrategy");
    this.pauseStrategy = pauseStrategy;
  }
}
