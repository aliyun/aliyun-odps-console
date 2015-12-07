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

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ship.DShipCommand;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by yichao on 15/9/8.
 */
public class DownloadTableTest {

  static String TABLE = "download_table_test";
  static String UPLOAD_PATH = DownloadPartitionTest.class.getResource("/partitions/blue_square.txt").getFile();
  static String PARENT_DIR = new File(UPLOAD_PATH).getParent() + "/";
  static String OUTPUT_DIR = DownloadPartitionTest.class.getResource("/").getPath();
  static String PATH = OUTPUT_DIR + TABLE + ".txt";


  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete(TABLE, true);
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("key", OdpsType.STRING));
    odps.tables().create(TABLE, tableSchema, true);
    String s = "tunnel u %s %s -cp=false -bs=1";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, UPLOAD_PATH, TABLE), context);
    assertNotNull(cmd);
    cmd.run();
  }

  @Test
  public void testDownloadWrongTable_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    try {
      cmd = DShipCommand.parse(String.format(s, "wrong_name", PATH), context);
      assertNotNull(cmd);
      cmd.run();
      Assert.fail("table not found");
    } catch(Exception e) {
      assertTrue("error message", e.getCause().getMessage().indexOf("Table not found") > 0);
    }
  }

  @Test
  public void testDownloadNonExistentPar_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    try {
      cmd = DShipCommand.parse(String.format(s, TABLE + "/non_exsistent_par='a'", PATH), context);
      assertNotNull(cmd);
      cmd.run();
      Assert.fail("download a non-existent partition");
    } catch(Exception e) {
      assertTrue("error message", e.getCause().getMessage().indexOf("can not specify partition for an unpartitioned table") >= 0);
    }
  }

  @Test
  public void testDownloadEmpty_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);

    String etable = "my_name_is_empty";
    if (!odps.tables().exists(etable)) {
      TableSchema tableSchema = new TableSchema();
      tableSchema.addColumn(new Column("key", OdpsType.STRING));
      odps.tables().create(etable, tableSchema, true);
    }

    String path = OUTPUT_DIR + etable + ".txt";
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, etable, path), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("read emtpy file", "", FileUtils.readFileToString(new File(path)));
  }

  @Test
  public void testDownloadEmpty_MultiThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    String etable = "my_name_is_empty";
    if (!odps.tables().exists(etable)) {
      TableSchema tableSchema = new TableSchema();
      tableSchema.addColumn(new Column("key", OdpsType.STRING));
      odps.tables().create(etable, tableSchema, true);
    }

    String path = OUTPUT_DIR + etable + ".txt";
    String s = "tunnel d %s %s -cp=false -threads 2";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, etable, path), context);
    assertNotNull(cmd);
    cmd.run();
    assertEquals("read emtpy file", "",
                 FileUtils
                     .readFileToString(new File(OUTPUT_DIR + etable + "/" + etable + "_0.txt")));
    assertEquals("read emtpy file", "",
                 FileUtils.readFileToString(new File(OUTPUT_DIR + etable + "/" + etable + "_1.txt")));
  }

  @Test
  public void testDownloadSequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();

    assertEquals("test download slice 1",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + ".txt")));
  }

  @Test
  public void testDownloadMultiThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -threads=2";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();

    assertEquals("test download slice 1",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_1_10.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + "_0.txt")));
    assertEquals("test download slice 2",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_11_20.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + "_1.txt")));
  }

  @Test
  public void testDownloadMaxRows_TwoThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -threads 2 -limit 15";
    DShipCommand cmd;

    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();

    assertEquals("test download slice 1",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_1_8.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + "_0.txt")));
    assertEquals("test download slice 2",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_9_15.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + "_1.txt")));
  }

  @Test
  public void testDownloadMaxRows_ThreeThreads() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -threads 3 -limit 4";
    DShipCommand cmd;

    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();

    assertEquals("test download slice 1",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_1_2.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + "_0.txt")));
    assertEquals("test download slice 2",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_3_4.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + "/" + TABLE + "_1.txt")));
    assertEquals("test download slice 3", "",
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE +"/" + TABLE + "_2.txt")));
  }

  @Test
  public void testDownloadMaxRows_Sequential() throws Exception {
    ExecutionContext context = ExecutionContext.init();
    String s = "tunnel d %s %s -cp=false -threads 1 -limit 15";
    DShipCommand cmd;
    cmd = DShipCommand.parse(String.format(s, TABLE, PATH), context);
    assertNotNull(cmd);
    cmd.run();

    assertEquals("test download slice 1",
                 FileUtils.readFileToString(new File(PARENT_DIR + "blue_square_1_15.txt")),
                 FileUtils.readFileToString(new File(OUTPUT_DIR + TABLE + ".txt")));
  }
}
