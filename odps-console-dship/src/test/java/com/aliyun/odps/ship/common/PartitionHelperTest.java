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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by yichao on 15/9/9.
 */
public class PartitionHelperTest {

  static String TABLE = "partition_helper_test";
  static String RED_SQUARE = "color='red',shape='square'";
  static String RED_CIRCLE = "color='red',shape='circle'";
  static String BLUE_SQUARE = "color='blue',shape='square'";
  static String BLUE_CIRCLE = "color='blue',shape='circle'";
  static String RED = "color='red'";
  static String CIRCLE = "shape='circle'";

  static PartitionHelper helper1;
  static PartitionHelper helper2;

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    DshipContext.INSTANCE.setExecutionContext(context);
    Odps odps = OdpsConnectionFactory.createOdps(context);

    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("key", OdpsType.STRING));
    tableSchema.addPartitionColumn(new Column("color", OdpsType.STRING));
    tableSchema.addPartitionColumn(new Column("shape", OdpsType.STRING));

    odps.tables().create(TABLE, tableSchema, true);
    odps.tables().get(TABLE).createPartition(new PartitionSpec(RED_SQUARE), true);
    odps.tables().get(TABLE).createPartition(new PartitionSpec(RED_CIRCLE), true);
    odps.tables().get(TABLE).createPartition(new PartitionSpec(BLUE_SQUARE), true);
    odps.tables().get(TABLE).createPartition(new PartitionSpec(BLUE_CIRCLE), true);

    odps.tables().create(TABLE + "_2", tableSchema, true);

    helper1 = new PartitionHelper(odps, odps.getDefaultProject(), TABLE);
    helper2 = new PartitionHelper(odps, odps.getDefaultProject(), TABLE + "_2");
  }

  @Test
  public void testBuildSuffix() throws Exception {
    PartitionSpec ps = new PartitionSpec("color='red',shape='square',age=12");
    assertEquals(".red.square.12", PartitionHelper.buildSuffix(ps));

    ps = new PartitionSpec("color='red'");
    assertEquals(".red", PartitionHelper.buildSuffix(ps));

    ps = new PartitionSpec("color='2:a',key2='3.b'");
    assertEquals(".2;a.3,b", PartitionHelper.buildSuffix(ps));

    ps = null;
    assertEquals("", PartitionHelper.buildSuffix(ps));
  }

  @Test
  public void testIsPartitioned() throws Exception {
    assertTrue(helper1.isPartitioned());
    assertTrue(helper2.isPartitioned());
  }

  @Test
  public void testPartitionSpecNull() throws Exception {
    Set<String> expectedSpecs = new HashSet<String>();
    expectedSpecs.add(RED_CIRCLE);
    expectedSpecs.add(RED_SQUARE);
    expectedSpecs.add(BLUE_SQUARE);
    expectedSpecs.add(BLUE_CIRCLE);


    Set<String> realSpecs = new HashSet<String>();
    for (PartitionSpec ps : helper1.completePartitionSpecs(null)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);

    realSpecs.clear();
    for (PartitionSpec ps : helper1.inferPartitionSpecs(null)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);

    // 无法推测出任何分区
    realSpecs.clear();
    expectedSpecs.clear();
    for (PartitionSpec ps : helper2.inferPartitionSpecs(null)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);
  }

  @Test
  public void testPartitionSpecColor() throws Exception {
    Set<String> expectedSpecs = new HashSet<String>();
    expectedSpecs.add(RED_CIRCLE);
    expectedSpecs.add(RED_SQUARE);

    Set<String> realSpecs = new HashSet<String>();
    for (PartitionSpec ps : helper1.completePartitionSpecs(RED)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);

    realSpecs.clear();
    for (PartitionSpec ps : helper1.inferPartitionSpecs(RED)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);
  }

  @Test
  public void testPartitionSpecShape() throws Exception {
    Set<String> expectedSpecs = new HashSet<String>();
    expectedSpecs.add(RED_CIRCLE);
    expectedSpecs.add(BLUE_CIRCLE);


    Set<String> realSpecs = new HashSet<String>();
    for (PartitionSpec ps : helper1.completePartitionSpecs(CIRCLE)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);

    expectedSpecs.clear();
    realSpecs.clear();
    for (PartitionSpec ps : helper1.inferPartitionSpecs(CIRCLE)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);
  }

  @Test
  public void testPartitionSpecBlueSquare() throws Exception {
    Set<String> expectedSpecs = new HashSet<String>();
    expectedSpecs.add(BLUE_SQUARE);

    Set<String> realSpecs = new HashSet<String>();
    for (PartitionSpec ps : helper1.completePartitionSpecs(BLUE_SQUARE)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);

    realSpecs.clear();
    for (PartitionSpec ps : helper1.inferPartitionSpecs(BLUE_SQUARE)) {
      realSpecs.add(ps.toString());
    }
    assertEquals(expectedSpecs, realSpecs);
  }
}
