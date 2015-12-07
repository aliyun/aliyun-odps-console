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

import java.util.ArrayList;
import java.util.List;

public class Args {

  /**
   * 把命令行文本切分为参数数组，去掉开头的命令本身
   * 
   * @param commandText命令行
   * @return String[] 参数数组
   * **/
  static public String[] split(String commandText) {

    if (commandText.trim().isEmpty())
      return new String[] {};

    String[] cmd = commandText.trim().split("-");
    List<String> args = new ArrayList<String>();
    // 去掉开头的命令
    for (int i = 1; i < cmd.length; i++) {
      if (cmd[i].length() > 0) {
        String[] options = cmd[i].trim().split(" ");

        String key = "-" + options[0];
        args.add(key);

        String value = "";
        for (int j = 1; j < options.length; j++)
          value += options[j];
        args.add(value);
      }
    }
    return args.toArray(new String[args.size()]);
  }
}
