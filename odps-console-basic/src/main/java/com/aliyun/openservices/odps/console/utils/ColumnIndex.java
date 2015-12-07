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

/**
 * 解析命令行中，对列索引数组做的各种处理
 * 
 * @author leheng.wang
 * **/
public class ColumnIndex {

  private Integer[] index;

  public ColumnIndex(String line) {
    parse(line);
  }

  public Integer[] get() {
    return index;
  }

  public void set(Integer[] idx) {
    index = idx;
  }

  public Boolean isEmpty() {
    return ((index == null) || (index.length == 0));
  }

  public Integer[] parse(String line) {
    String[] args = line.trim().split(",");
    List<Integer> L = new ArrayList<Integer>();
    for (int i = 0; i < args.length; i++) {
      String[] idx = args[i].split(":");
      if (idx.length < 2)
        L.add(Integer.parseInt(idx[0]));
      else
        for (int j = Integer.parseInt(idx[0]); j <= Integer.parseInt(idx[1]); j++)
          L.add(j);
    }
    index = L.toArray(new Integer[L.size()]);
    return index;
  }

  public Boolean validate(int min, int max) {
    if ((index != null) && (index.length > 0))
      for (int i = 0; i < index.length; i++)
        if ((index[i] < min) || (index[i] > max))
          return false;

    return true;
  }

  public String toString() {
    String line = new String();
    if ((index != null) && (index.length > 0)) {
      for (int i = 0; i < index.length - 1; i++) {
        line = line + index[i].toString() + ',';
      }
      line = line + index[index.length - 1].toString();
    }
    ;
    return line;
  }

}
