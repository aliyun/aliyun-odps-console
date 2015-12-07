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

package com.aliyun.openservices.odps.console.datahub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class DatahubCommandTest {

  @Test
  public void testLoadShard() throws Exception {
    DatahubCommand command = DatahubCommand
        .parse(
            "hub load 3 shards on test_dx.test_log  http://xxxxxxx",
            null);
    assertNotNull(command);
    assertEquals(3, command.shardNumber);
    assertEquals(DatahubCommand.DatahubMethod.Load, command.method);
    assertEquals("test_dx", command.projectName);
    assertEquals("test_log", command.tableName);
    assertEquals("http://xxxxxxx", command.endpoint);

    try {
      command = DatahubCommand
          .parse(
              "hub load 3 shards on test_dx. ",
              null);
      fail("need fail");
    } catch (Exception e) {
      System.out.println(e.toString());
    }

    try {
      command = DatahubCommand
          .parse(
              "hub load 3 shards on  .test_log",
              null);
      fail("need fail");
    } catch (Exception e) {
      System.out.println(e.toString());
    }

    try {
      command = DatahubCommand
          .parse(
              "hub load d shards on  test_dx.test_log",
              null);
      fail("need fail");
    } catch (Exception e) {
      System.out.println(e.toString());
    }
  }

  @Test
  public void testUnLoadShard() throws Exception {
    DatahubCommand command = DatahubCommand
        .parse(
            "hub unload shard on test_dx.test_log http://xxxxxxx",
            null);
    assertNotNull(command);
    assertEquals(DatahubCommand.DatahubMethod.Unload, command.method);
    assertEquals("test_dx", command.projectName);
    assertEquals("test_log", command.tableName);
    assertEquals("http://xxxxxxx", command.endpoint);

    try {
      command = DatahubCommand
          .parse(
              "hub unload 3 shard on  test_dx.test_log",
              null);
      fail("need fail");
    } catch (Exception e) {
      System.out.println(e.toString());
    }
  }

  @Test
  public void testShardStatus() throws Exception {
    DatahubCommand command = DatahubCommand
        .parse(
            "hub  shard status on  test_dx.test_log  http://xxxxxx",
            null);
    assertNotNull(command);
    assertEquals(DatahubCommand.DatahubMethod.ShardStatus, command.method);
    assertEquals("test_dx", command.projectName);
    assertEquals("test_log", command.tableName);
    assertEquals("http://xxxxxx", command.endpoint);
  }

  @Test
  public void testShardList() throws Exception {
    DatahubCommand command = DatahubCommand
        .parse(
            "hub shard list on test_dx.test_log http://xxxxxxx",
            null);
    assertNotNull(command);
    assertEquals(DatahubCommand.DatahubMethod.ShardList, command.method);
    assertEquals("test_dx", command.projectName);
    assertEquals("test_log", command.tableName);
    assertEquals("http://xxxxxxx", command.endpoint);
  }

  @Test
  public void testReplicateStatus() throws Exception {
    DatahubCommand command = DatahubCommand
        .parse(
            "hub replicate status of shard 0 on test_dx.test_log http://xxxxxxx",
            null);
    assertNotNull(command);
    assertEquals(DatahubCommand.DatahubMethod.ReplicateStatus, command.method);
    assertEquals("test_dx", command.projectName);
    assertEquals("test_log", command.tableName);
    assertEquals("http://xxxxxxx", command.endpoint);

    try {
      command = DatahubCommand
          .parse(
              "hub replicate status of shard two on test_dx.test_log",
              null);
      fail("need fail");
    } catch (Exception e) {
      System.out.println(e.toString());
    }
  }
}
