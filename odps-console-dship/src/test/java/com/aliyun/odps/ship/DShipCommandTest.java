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

package com.aliyun.odps.ship;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/5/19.
 */
public class DShipCommandTest {

  @Test
  public void testDshipMultiThreads() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete("instances", true);
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("key", OdpsType.STRING));
    odps.tables().create("instances", tableSchema, true);
    odps.tables().get("instances").truncate();
    String file = this.getClass().getResource("/many_record.txt").getFile();
    System.out.println(file);
    DShipCommand command = DShipCommand.parse(
        "tunnel u " + file + " instances -cp=false -threads=4  -bs=1", context);
    command.run();

    String outputFile = new File(file).getParent() + "/abcd.txt";

    command = DShipCommand.parse("tunnel d instances " + outputFile + " -cp=false -threads=4 -limit=580000", context);
    command.run();

    testRecordDelimiter();
  }

  @Test
  public void testDshipMultiPartitions() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);

    odps.tables().delete("multi_partitions", true);

    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("key", OdpsType.STRING));
    tableSchema.addPartitionColumn(new Column("pt", OdpsType.BIGINT));

    odps.tables().create("multi_partitions", tableSchema, true);
    odps.tables().get("multi_partitions").createPartition(new PartitionSpec("pt=1"), true);
    odps.tables().get("multi_partitions").createPartition(new PartitionSpec("pt=2"), true);
    odps.tables().get("multi_partitions").createPartition(new PartitionSpec("pt=3"), true);

    String file = this.getClass().getResource("/many_record.txt").getFile();
    System.out.println(file);
    DShipCommand command = DShipCommand.parse(
        "tunnel u " + file + " multi_partitions/pt=1 -cp=false -threads=4  -bs=1", context);
    command.run();

    command = DShipCommand.parse(
        "tunnel u " + file + " multi_partitions/pt=2 -cp=false -threads=4  -bs=1", context);
    command.run();

    command = DShipCommand.parse(
        "tunnel u " + file + " multi_partitions/pt=3 -cp=false -threads=4  -bs=1", context);
    command.run();

    String outputFile = new File(file).getParent() + "/abcd.txt";

    command = DShipCommand.parse("tunnel d multi_partitions " + outputFile + " -cp=false -threads=2 -limit=1700000", context);
    command.run();
  }


  @Test
  public void testHelpCommand() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    DShipCommand command = DShipCommand.parse("tunnel help upload", context);
    String result = ODPSConsoleUtils.runCommand(command);
    System.out.println(result);
    assertFalse(result.contains("--"));
  }

  @Test
  public void testDshipUploadFolder() throws OdpsException, ODPSConsoleException, IOException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    String project = odps.getDefaultProject();
    odps.tables().delete("instances", true);
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("key", OdpsType.STRING));
    odps.tables().create("instances", tableSchema, true);
    odps.tables().get("instances").truncate();
    String file = this.getClass().getResource("/file/fileuploader/foldertest").getFile();
    System.out.println(file);
    DShipCommand command = DShipCommand.parse(
        "tunnel u " + file + " instances -dbr true", context);
    command.run();

    String path = this.getClass().getResource("/").getFile() + "emptyfolder4test";
    File emptyFolder = new File(path);
    if (emptyFolder.exists()) {
      FileUtils.forceDelete(emptyFolder);
    }

    assertTrue(emptyFolder.mkdir());

    command = DShipCommand.parse(
        "tunnel u " + path + " instances -dbr true", context);
    command.run();
  }

  public void testRecordDelimiter() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);

    String file = this.getClass().getResource("/many_record.txt").getFile();

    String outputFile = new File(file).getParent() + "/abcd1.txt";

    // using windows style file
    DShipCommand command = DShipCommand.parse("tunnel d instances " + outputFile + " -cp=false -limit=580 -rd \"\\r\\n\"", context);
    command.run();

    command = DShipCommand.parse(
        "tunnel u " + outputFile + " instances -cp=false", context);
    command.run();
    assertEquals(DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER), "\r\n");

    // using linux style file
    outputFile = new File(file).getParent() + "/abcd2.txt";
    command = DShipCommand.parse("tunnel d instances " + outputFile + " -cp=false -limit=600 -rd \"\\n\"", context);
    command.run();

    command = DShipCommand.parse(
        "tunnel u " + outputFile + " instances -cp=false", context);
    command.run();
    assertEquals(DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER), "\n");
  }

  @Test
  public void testSpecialDelimiter() throws OdpsException, ODPSConsoleException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);

    String tableName = "dship_special_delimiter_test_table";
    odps.tables().delete(tableName, true);
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("name", OdpsType.STRING));
    tableSchema.addColumn(new Column("num", OdpsType.BIGINT));
    odps.tables().create(tableName, tableSchema, true);

    String file = this.getClass().getResource("/special_delimiter_record.txt").getFile();

    DShipCommand command = DShipCommand.parse(
        "tunnel u " + file + " " + tableName + " -fd=\"\\u0002\" -rd=\"\\u0001\"", context);
    command.run();

    String outputFile = new File(file).getParent() + "/special.txt";

    command = DShipCommand.parse("tunnel d " + tableName + " " + outputFile + " -fd \"\\u0002\" -rd \"\\u0001\"", context);
    command.run();

    assertArrayEquals(DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER).getBytes(),
                      new byte[]{1});
    assertArrayEquals(DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER).getBytes(), new byte[]{2});
  }


  public String getNewTypeProjectName() throws IOException {
    InputStream is = null;
    try {
      is = DShipCommandTest.class.getClassLoader().getResourceAsStream("odps_config.ini");
      Properties props = new Properties();
      props.load(is);

      return props.getProperty("new_type_project_name");
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  @Test
  public void testNewBasicType() throws ODPSConsoleException, OdpsException, IOException {

    ExecutionContext context = ExecutionContext.init();

    String newTypeTestProject = getNewTypeProjectName();

    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.setDefaultProject(newTypeTestProject);

    String tablename = "test_basic_new_type_1";
    odps.tables().delete(tablename, true);

    Map<String, String> hints = new HashMap<String, String>();
    hints.put("odps.sql.hive.compatible", "true");
    hints.put("odps.sql.preparse.odps2", "hybrid");
    hints.put("odps.sql.planner.mode", "lot");
    hints.put("odps.sql.planner.parser.odps2", "true");
    hints.put("odps.sql.ddl.odps2", "true");
    hints.put("odps.compiler.output.format", "lot,pot");

    // new SQLTask
    // tinyint, smallint, int, float, decimal(20,10), char(10), varchar(10), binary, timestamp, date
    String sql = "create table " + tablename + " (ti tinyint, si smallint, ii int, fl float, de decimal(20, 10), cc char(10), vc varchar(10), bi binary, da date, tim timestamp);";
    Instance i = SQLTask.run(odps, newTypeTestProject, sql, hints, null);
    i.waitForSuccess();

    // upload normal
    String file = this.getClass().getResource("/test_new_type_data.txt").getFile();
    System.out.println(file);
    DShipCommand command = DShipCommand.parse(
        "tunnel u " + file + "  " + newTypeTestProject + "." + tablename , context);
    command.run();

    String outputFile = new File(file).getParent() + "/expect_tmp.txt";
    command = DShipCommand.parse("tunnel d " + newTypeTestProject + "." + tablename + " " + outputFile, context);
    command.run();

    assertEquals("download", FileUtil.getStringFromFile("src/test/resources/test_new_type_data.txt"),
                 FileUtil.getStringFromFile(outputFile));

    odps.tables().delete(newTypeTestProject, tablename);
  }

  @Test
  public void testDownloadPartialColumns() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    String tablename = "test_download_partial_columns";
    odps.tables().delete(tablename, true);
    TableSchema tableSchema = new TableSchema();
    tableSchema.addColumn(new Column("i1", OdpsType.BIGINT));
    tableSchema.addColumn(new Column("s1", OdpsType.STRING));
    tableSchema.addColumn(new Column("d1", OdpsType.DATETIME));
    tableSchema.addColumn(new Column("b1", OdpsType.BOOLEAN));
    tableSchema.addColumn(new Column("de1", OdpsType.DOUBLE));
    tableSchema.addColumn(new Column("doub1", OdpsType.DECIMAL));

    odps.tables().create(tablename, tableSchema, true);
    odps.tables().get(tablename).truncate();
    String file = this.getClass().getResource("/file/filedownloader/sample.txt").getFile();
    System.out.println(file);
    DShipCommand command = DShipCommand.parse(
        "tunnel u " + file + "  " + tablename + " -fd=|| -dfp=yyyyMMddHHmmss", context);
    command.run();

    // normal download 0,1,3,4,5 using -ci.
    String outputFile = new File(file).getParent() + "/expect.txt";
    command = DShipCommand.parse("tunnel d " + tablename + " " + outputFile + " -ci=0,1,3,4,5 -fd=|| -dfp=yyyyMMddHHmmss", context);
    command.run();

    assertEquals("download", FileUtil.getStringFromFile("src/test/resources/file/filedownloader/sample_partial.txt"),
                 FileUtil.getStringFromFile(outputFile));

    // normal download 0,1,3,4,5 using -cn.
    command = DShipCommand.parse("tunnel d " + tablename + " " + outputFile + " -cn=i1,s1,b1,de1,doub1  -fd=|| -dfp=yyyyMMddHHmmss", context);
    command.run();

    assertEquals("download", FileUtil.getStringFromFile("src/test/resources/file/filedownloader/sample_partial.txt"),
                 FileUtil.getStringFromFile(outputFile));

    // error download column not exist
    command =
        DShipCommand.parse("tunnel d " + tablename + " " + outputFile + " -cn=notin  -fd=|| -dfp=yyyyMMddHHmmss", context);
    try {
      command.run();
      fail("need fail, column not exists");
    } catch (ODPSConsoleException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
      Assert.assertTrue(e.getCause().getMessage().contains("No such column:notin"));
    }

    // error download column not exist
    command =
        DShipCommand.parse("tunnel d " + tablename + " " + outputFile + " -ci=8  -fd=|| -dfp=yyyyMMddHHmmss", context);
    try {
      command.run();
      fail("need fail, column not exists");
    } catch (ODPSConsoleException e) {
      Assert.assertTrue(e.getCause() instanceof IllegalArgumentException);
      Assert.assertTrue(e.getCause().getMessage().contains("idx out of range"));
    }
  }

}
