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

package com.aliyun.odps.ship.download;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ship.DShipCommand;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by yichao on 15/9/8.
 */
public class DownloadPartitionTest {

  static String TABLE = "download_partition_test";
  static String UPLOAD_PATH = DownloadPartitionTest.class.getResource("/partitions/blue_square.txt").getFile();
  static String PARENT_DIR = new File(UPLOAD_PATH).getParent() + "/";
  static String OUTPUT_DIR = DownloadPartitionTest.class.getResource("/").getPath();
  static String PATH = OUTPUT_DIR + TABLE + ".txt";

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    String red_square = "color='red',shape='square'";
    String red_circle = "color='red',shape='circle'";
    String blue_square = "color='blue',shape='square'";
    String blue_circle = "color='blue',shape='circle'";

    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete(TABLE, true);
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("key", OdpsType.STRING));
    tableSchema.addPartitionColumn(new Column("color", OdpsType.STRING));
    tableSchema.addPartitionColumn(new Column("shape", OdpsType.STRING));

    odps.tables().create(TABLE, tableSchema, true);
    odps.tables().get(TABLE).createPartition(new PartitionSpec(red_square), true);
    odps.tables().get(TABLE).createPartition(new PartitionSpec(red_circle), true);
    odps.tables().get(TABLE).createPartition(new PartitionSpec(blue_square), true);
    odps.tables().get(TABLE).createPartition(new PartitionSpec(blue_circle), true);

    String s = "tunnel u %s %s -cp=false -bs=1";
    DShipCommand cmd;
    cmd = DShipCommand.parse(
        String.format(s, PARENT_DIR + "red_square.txt", TABLE + "/" + red_square), context);
    assertNotNull(cmd);
    cmd.run();
    cmd = DShipCommand.parse(
        String.format(s, PARENT_DIR + "red_circle.txt", TABLE + "/" + red_circle), context);
    assertNotNull(cmd);
    cmd.run();
    cmd = DShipCommand.parse(
        String.format(s, PARENT_DIR + "blue_square.txt", TABLE + "/" + blue_square), context);
    assertNotNull(cmd);
    cmd.run();
    cmd = DShipCommand.parse(
        String.format(s, PARENT_DIR + "blue_circle.txt", TABLE + "/" + blue_circle), context);
    assertNotNull(cmd);
    cmd.run();

  }

  @Test
  public void testDownloadEmpty_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    String etable = "my_name_is_red";
    if (!odps.tables().exists(etable)) {
      TableSchema tableSchema = new TableSchema();
      tableSchema.addColumn(new Column("key", OdpsType.STRING));
      tableSchema.addPartitionColumn(new Column("color", OdpsType.STRING));
      odps.tables().create(etable, tableSchema, true);
      odps.tables().get(etable).createPartition(new PartitionSpec("color='orange'"), true);
      odps.tables().get(etable).createPartition(new PartitionSpec("color='gray'"), true);
    }

    String path = OUTPUT_DIR + etable + ".txt";
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, etable, path), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("read emtpy file orange", "",
                 FileUtils.readFileToString(new File(OUTPUT_DIR + etable + "/" + etable + ".orange.txt")));
    assertEquals("read emtpy file gray", "",
                 FileUtils.readFileToString(new File(OUTPUT_DIR + etable +"/" + etable + ".gray.txt")));
  }

  @Test
  public void testDownloadEmpty_MultiThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    String etable = "my_name_is_red";
    if (!odps.tables().exists(etable)) {
      TableSchema tableSchema = new TableSchema();
      tableSchema.addColumn(new Column("key", OdpsType.STRING));
      tableSchema.addPartitionColumn(new Column("color", OdpsType.STRING));
      odps.tables().create(etable, tableSchema, true);
      odps.tables().get(etable).createPartition(new PartitionSpec("color='orange'"), true);
      odps.tables().get(etable).createPartition(new PartitionSpec("color='gray'"), true);
    }

    String path = OUTPUT_DIR + etable + ".txt";
    String s = "tunnel d %s %s -cp=false -threads 2";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, etable, path), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("read emtpy file", "",
                 FileUtils.readFileToString(
                     new File(OUTPUT_DIR + etable + "/" + etable + ".orange.txt")));
    assertEquals("read emtpy file", "",
                 FileUtils.readFileToString(new File(OUTPUT_DIR + etable +"/" + etable + ".gray.txt")));
  }


  @Test
  public void testDownloadOnePar_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE + "/color='blue',shape='square'", PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + ".txt")));
  }

  @Test
  public void testDownloadOnePar_MultiThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -threads 2";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE + "/color='blue',shape='square'", PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square slice 1",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_1_10.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + "_0.txt")));
    assertEquals("test dowload blue square slice 2",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_11_20.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + "_1.txt")));
  }

  @Test
  public void testDownloadTwoPar_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE + "/color='blue'", PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square.txt")),
                 FileUtils.readFileToString(
                     new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.square.txt")));
    assertEquals("test dowload blue circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_circle.txt")),
                 FileUtils.readFileToString(
                     new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.circle.txt")));

    try {
      cmd = DShipCommand.parse(String.format(s, TABLE + "/shape='square'", PATH), context);
      assertNotNull(cmd);
      cmd.run();
      fail("need fail");
    } catch (ODPSConsoleException e) {
      assertTrue("error message", e.getCause().getMessage().indexOf("ERROR: can not infer any partitions from:") == 0);
    }
  }

  @Test
  public void testDownloadWrongPar_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    try {
      cmd = DShipCommand.parse(String.format(s, TABLE + "/color='black'", PATH), context);
      assertNotNull(cmd);
      cmd.run();
      fail("need fail");
    } catch (ODPSConsoleException e) {
      assertTrue("error message", e.getCause().getMessage().indexOf("ERROR: can not infer any partitions from:") == 0);
    }
  }

  @Test
  public void testDownloadFourPar_TwoThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -threads 2";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square.txt")),
                 FileUtils.readFileToString(
                     new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.square.txt")));
    assertEquals("test dowload blue circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_circle.txt")),
                 FileUtils.readFileToString(
                     new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.circle.txt")));
    assertEquals("test dowload red square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "red_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".red.square.txt")));
    assertEquals("test dowload red circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "red_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".red.circle.txt")));
  }

  @Test
  public void testDownloadFourPar_Squential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.square.txt")));
    assertEquals("test dowload blue circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.circle.txt")));
    assertEquals("test dowload red square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "red_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".red.square.txt")));
    assertEquals("test dowload red circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "red_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".red.circle.txt")));
  }


  @Test
  public void testDownloadFourPar_ThreeThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -threads 3";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.square.txt")));
    assertEquals("test dowload blue circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.circle.txt")));
    assertEquals("test dowload red square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "red_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".red.square.txt")));
    assertEquals("test dowload red circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "red_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".red.circle.txt")));
  }

  @Test
  public void testDownloadFourPar_FiveThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -threads 5";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.square.txt")));
    assertEquals("test dowload blue circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.circle.txt")));
    assertEquals("test dowload red square",
                 FileUtils.readFileToString(new File(PARENT_DIR + "red_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".red.square.txt")));
    assertEquals("test dowload red circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "red_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".red.circle.txt")));
  }

  @Test
  public void testDownloadMaxRows_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -limit 35";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE + "/color='blue'", PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square 15",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_1_15.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.square.txt")));
    assertEquals("test dowload blue circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.circle.txt")));
  }

  @Test
  public void testDownloadMaxRows_MultiThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -limit 35 -threads 2";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE + "/color='blue'", PATH), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("test dowload blue square 15",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_1_15.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.square.txt")));
    assertEquals("test dowload blue circle",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_circle.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + ".blue.circle.txt")));
  }
}
