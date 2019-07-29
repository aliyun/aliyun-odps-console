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

import com.aliyun.odps.Instance;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

/**
 * Created by zhenhong.gzh on 15/8/17.
 *
 * 重写 getProgressMessage 方法来获取 日志
 */

public abstract class XFlowProgressHelper {
  //获取日志的时间间隔
  public int interval;

  public int getInterval() {
    return interval;
  }

  public void setInterval(int interval) {
    this.interval = interval;
  }

  // config
  public String config;

  public String getConfig() {
    return config;
  }

  public void setConfig(final String config) {
    this.config = config;
  }

  public abstract String getPAIAlgoName();


  /**
   * 获取 instance log 格式化后的输出
   *
   * @param instance
   * @return 日志的格式化输出字符串
   */

  public abstract String getProgressMessage(Instance instance) throws ODPSConsoleException, OdpsException;

  public abstract boolean needProgressMessage(Instance instance) throws ODPSConsoleException, OdpsException;

}
