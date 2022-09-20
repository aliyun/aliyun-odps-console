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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by yichao on 15/9/9.
 */
public class PartitionHelper {

  private Table table;
  private boolean isPartitioned;

  public PartitionHelper(
      Odps odps,
      String projectName,
      String schemaName,
      String tableName) throws OdpsException {

    table = odps.tables().get(projectName, schemaName, tableName);
    isPartitioned = table.isPartitioned();
  }

  public boolean isPartitioned() {
    return isPartitioned;
  }

  /**
   * 为 PartitionSpec 文件名建立后缀，用于区分不同的文件
   *
   * ParVal 允许字符 'a'-'z', 'A'-'Z', '0'-'9', ':_-$#.@! '
   * Windows 文件名不允许字符：'/\:*?"<>|'
   *
   * @param ps
   * @return {key1:val1, key2:val2} => ".val1.val2"。如果 ps 为 null，返回空字符串
   */
  public static String buildSuffix(PartitionSpec ps) {
    if (ps == null) {
      return "";
    }

    String path = "";
    ps.keys().size();
    for (String key : ps.keys()) {
      String val = ps.get(key);
      val = val.replace(':', ';');
      val = val.replace('.', ',');
      path += '.' + val;
    }
    return path;
  }

  /**
   * 当用户指定一个 PartitionSpec 超集时(一个不完整的 PS)，推导出 PartitionSpec 集合
   * 当指定的 PartitionSpec 不存在时，返回一个空集
   * 为了保证顺序，返回一个列表
   */
  public List<PartitionSpec> completePartitionSpecs(String partitionSpecLiteral) {

    if (!isPartitioned) {
      throw new IllegalArgumentException(Constants.ERROR_INDICATOR
                                         + "can not complete PartitionSpecs for an unpartitioned table");
    }

    PartitionSpec ps = (partitionSpecLiteral == null) ? null : new PartitionSpec(partitionSpecLiteral);

    // 用于过滤 Partition
    // 当 partitionSpecLiteral = null，对应的 KV={}，表示用户要下载 table 的所有 partition
    Map<String, String> filterKV = getKV(ps);

    // 一个 table 的 PartitionSpec 以 KV 形式来描述
    // 例如 {color:red, shape:square}，{color:red, shape:circle}
    // 当它是 filterKV 的一个子集时，则认为这是一个将被下载的 PS，加入到 parSpec 中
    List<PartitionSpec> partitionSpecs = new ArrayList<PartitionSpec>();
    for (Partition p : table.getPartitions()) {
      PartitionSpec parSpec = p.getPartitionSpec();
      Map<String, String> parKV = getKV(parSpec);
      if (isSupersetOf(filterKV, parKV)) {
        partitionSpecs.add(parSpec);
      }
    }

    return partitionSpecs;
  }

  /**
   * 只支持第一级的 Partition 匹配
   *
   * 当用户指定一个 PartitionSpec 超集时(一个不完整的 PS)，推导出 PartitionSpec 集合
   * 当指定的 PartitionSpec 不存在时，返回一个空集
   * 为了保证顺序，返回一个列表
   */
  public List<PartitionSpec> inferPartitionSpecs(String partitionSpecLiteral) throws OdpsException {

    if (!isPartitioned) {
      throw new IllegalArgumentException(Constants.ERROR_INDICATOR
                                         + "can not infer PartitionSpecs for an unpartitioned table");
    }

    PartitionSpec ps = (partitionSpecLiteral == null) ? null : new PartitionSpec(partitionSpecLiteral);

    Iterator<Partition> it = table.getPartitionIterator(ps);
    List<PartitionSpec> partitions = new ArrayList<PartitionSpec>();
    try {
      while (it.hasNext()) {
        partitions.add(it.next().getPartitionSpec());
      }
    } catch (RuntimeException e) {
    }

    return partitions;
  }

  /**
   * PartitionSpec 不提供接口来获取 hash map，所以必须手动生成
   * ps 允许为 null，此时返回空的 KV 集合 {}
   */
  private static Map<String, String> getKV(PartitionSpec ps) {
    Map<String, String> kv = new LinkedHashMap<String, String>();
    if (ps != null) {
      for (String key : ps.keys()) {
        kv.put(key, ps.get(key));
      }
    }
    return kv;
  }

  /**
   * 判断 x 是否表示 y 的一个超集
   * 如果 x 是 y 的超集，那么 x 中的每个 key 对应的 value 都应该等于这个 key 在 y 中的 value
   * 例如 {y:b} 是 {x:a, y:b, z:c} 的超集
   * {} 是任何集合的超集
   *
   * @param x
   * @param y
   * @return 所有的 value 都相等返回 true，只要有一个 value 相等，就返回 false
   */
  private static boolean isSupersetOf(Map<String, String> x, Map<String, String> y) {
    for (String key : x.keySet()) {
      String expectVal = x.get(key);
      if (!expectVal.equals(y.get(key))) {
        return false;
      }
    }
    return true;
  }
}
