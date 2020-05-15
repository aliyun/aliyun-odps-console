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

package com.aliyun.odps.ship.optionparser;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.ship.common.Constants;
import com.aliyun.odps.ship.common.DshipContext;
import com.aliyun.odps.ship.common.OptionsBuilder;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * 命令行partition格式测试
 * */
public class ParsePartitionSpecTest {
  private static final String TEST_TABLE_NAME = "parse_partition_spec_test";
  private static String projectName;

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    projectName = context.getProjectName();
    DshipContext.INSTANCE.setExecutionContext(context);
    Odps odps = OdpsConnectionFactory.createOdps(context);

    TableSchema schema = new TableSchema();
    schema.addColumn(new Column("col1", TypeInfoFactory.STRING));
    odps.tables().create(TEST_TABLE_NAME, schema, true);
  }

  @AfterClass
  public static void tearDown() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
    Odps odps = OdpsConnectionFactory.createOdps(context);
    odps.tables().delete(TEST_TABLE_NAME, true);
  }

  /**
   * 测试正确设置project.table/partition格式
   * */
  @Test
  public void testPartitionSpec() throws Exception {

    String[] args;
    args =
        new String[] {"upload", "src/test/resources/test_data.txt",
            projectName + "." + TEST_TABLE_NAME + "/ds='2113',pt='pttest'"};

    OptionsBuilder.buildUploadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String project = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("project name not equal", projectName, project);
    assertEquals("table name not equal", TEST_TABLE_NAME, table);
    assertEquals("partition spec not equal", "ds='2113',pt='pttest'", partition);

  }

  /**
   * 测试partition中包含空格，正常
   * */
  @Test
  public void testPartitionSpecValueWithSpace() throws Exception {

    String[] args;
    args =
        new String[] {"upload", "src/test/resources/test_data.txt",
                      projectName + "." + TEST_TABLE_NAME + "/ds='21 13',pt='ptt est'"};

    OptionsBuilder.buildUploadOption(args);
    String source =  DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String project = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("project name not equal", projectName, project);
    assertEquals("table name not equal", TEST_TABLE_NAME, table);
    assertEquals("partition spec not equal", "ds='21 13',pt='ptt est'", partition);

  }

  /**
   * 测试partition之前逗号前有空格
   * **/
  @Test
  public void testPartitionCommaWithSpace() throws Exception {

    String[] args;
    args =
        new String[] {"upload", "src/test/resources/test_data.txt",
                      projectName + "." + TEST_TABLE_NAME + "/ds ='21 13 ' , pt =' ptt est '"};

    OptionsBuilder.buildUploadOption(args);
    String source = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
    String project = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    String table = DshipContext.INSTANCE.get(Constants.TABLE);
    String partition = DshipContext.INSTANCE.get(Constants.PARTITION_SPEC);
    assertEquals("source not equal", "src/test/resources/test_data.txt", source);
    assertEquals("project name not equal", projectName, project);
    assertEquals("table name not equal", TEST_TABLE_NAME, table);
    assertEquals("partition spec not equal", "ds='21 13 ',pt=' ptt est '", partition);

  }

  /**
   * 测试partition格式非法
   * **/
  @Test
  public void testFailIllegalPartition() throws Exception {

    String[] args;
    args =
        new String[] {"upload", "src/test/resources/test_data.txt",
                      projectName + "." + TEST_TABLE_NAME + "/ds ='21=13' , pt ='ptt=est'"};

    try {
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (IllegalArgumentException e) {
      assertTrue("Unrecognized partition", e.getMessage().indexOf("Invalid partition spec") == 0);
    }

    args =
        new String[] {"upload", "src/test/resources/test_data.txt",
                      projectName + "." + TEST_TABLE_NAME + "/ds ='21,13' , pt ='ptt,est'"};
    try {
      OptionsBuilder.buildUploadOption(args);
      fail("need fail");
    } catch (IllegalArgumentException e) {
      assertTrue("Unrecognized partition", e.getMessage().indexOf("Invalid partition spec") == 0);
    }
  }
  
//  /**
//   * 测试partition格式非法
//   * **/
//  @Test
//  public void testFailIllegalPartitionWithNull() throws Exception {
//
//    String[] args;
//    args =
//        new String[] {"upload", "src/test/resources/test_data.txt",
//            "up_test_project.test_table/"};
//
//    try {
//      Map<String, String> context = OptionsBuilder.buildUploadOption(args);
//      fail("need fail");
//    } catch (IllegalArgumentException e) {
//      assertTrue("Unrecognized partition", e.getMessage().indexOf("Unrecognized partition") == 0);
//    }
//
//  }

}
