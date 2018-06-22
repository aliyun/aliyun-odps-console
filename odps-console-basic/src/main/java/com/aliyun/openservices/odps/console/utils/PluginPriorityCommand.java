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

package com.aliyun.openservices.odps.console.utils;

/**
 * 优先级命令：封装命令名称和优先级权重。
 * 优先级取值范围：[Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY]
 * 使用常量 MAX_PRIORITY 和 MIN_PRIORITY 表示
 * Created by zhenhong.gzh on 2015/6/10.
 */
public class PluginPriorityCommand implements Comparable<PluginPriorityCommand> {

  public static final float MAX_PRIORITY = Float.POSITIVE_INFINITY;
  public static final float MIN_PRIORITY = Float.NEGATIVE_INFINITY;

  private String commandName;
  private float commandPriority;

  public PluginPriorityCommand(String commandName, float commandPriority) {

    if (commandPriority >= MIN_PRIORITY && commandPriority <= MAX_PRIORITY) {
      this.commandName = commandName;
      this.commandPriority = commandPriority;
    } else {
      throw new IllegalArgumentException(
          "Argument illegal. Command name is: " + commandName + "; command priority is: "
          + commandPriority);
    }
  }

  /**
   * 获取命令对象名称
   *
   * @param
   * @return String 命令名称
   * *
   */
  public String getCommandName() {
    return this.commandName;
  }

  /**
   * 获取命令对象的优先级权重值
   *
   * @param
   * @return float 命令优先级
   * *
   */
  public float getCommandPriority() {
    return this.commandPriority;
  }

  @Override
  public int compareTo(PluginPriorityCommand o1) {
    return Float.compare(o1.getCommandPriority(), this.commandPriority);
  }
}
