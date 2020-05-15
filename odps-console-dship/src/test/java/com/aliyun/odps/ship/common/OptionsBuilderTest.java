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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

/**
 * Created by nizheming on 15/6/9.
 */
public class OptionsBuilderTest {
  private final static String TEST_TABLE_NAME = "options_builder_test";

  @BeforeClass
  public static void setup() throws ODPSConsoleException, OdpsException {
    ExecutionContext context = ExecutionContext.init();
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

  @Test(expected = IllegalArgumentException.class)
  public void testCheckParameters() throws Exception {
    OptionsBuilder.buildUploadOption(
        new String[]{"upload", "src/test/resources/many_record.txt", TEST_TABLE_NAME});
    DshipContext.INSTANCE.put(Constants.THREADS, "1");

    try {
      OptionsBuilder.checkParameters("upload");// exception: "Record delimiter is null"
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Record delimiter is null"));
      throw e;
    }
    Assert.assertTrue(false);
  }

  @Test
  public void testCheckParametersWithRD() throws Exception {
    OptionsBuilder.buildUploadOption(
        new String[]{"upload", "src/test/resources/many_record.txt", TEST_TABLE_NAME, "-rd=\n"});
    DshipContext.INSTANCE.put(Constants.THREADS, "1");
    OptionsBuilder.checkParameters("upload");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThreadsNeg1() throws Exception {
    OptionsBuilder.buildUploadOption(
        new String[]{"upload", "src/test/resources/many_record.txt", TEST_TABLE_NAME});
    DshipContext.INSTANCE.put(Constants.THREADS, "-1");
    OptionsBuilder.checkParameters("upload");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThreadsNeg2() throws Exception {
    OptionsBuilder.buildUploadOption(
        new String[]{"upload", "src/test/resources/many_record.txt", TEST_TABLE_NAME});
    DshipContext.INSTANCE.put(Constants.THREADS, "1.5");
    OptionsBuilder.checkParameters("upload");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testThreadsNeg3() throws Exception {
    OptionsBuilder.buildUploadOption(
        new String[]{"upload", "src/test/resources/many_record.txt", TEST_TABLE_NAME});
    DshipContext.INSTANCE.put(Constants.THREADS, "0");
    OptionsBuilder.checkParameters("upload");
  }

  @Test
  public void testUnescapeDelimiter() throws Exception {
    OptionsBuilder.buildUploadOption(
        new String[]{"upload", "src/test/resources/many_record.txt", TEST_TABLE_NAME, "-rd",
                     "a \tb\n\\u0001\r\n\\u0003", "-fd", "\\u0002"});
    OptionsBuilder.checkParameters("upload");

    org.junit.Assert.assertArrayEquals(new byte[]{97, 32, 9, 98, 10, 1, 13, 10, 3},
                                       DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER)
                                           .getBytes());

    org.junit.Assert.assertArrayEquals(new byte[]{2},
                                       DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER)
                                           .getBytes());
  }

  @Test
  public void testDownloadExponetial() throws Exception {
    OptionsBuilder.buildDownloadOption(
        new String[]{"download", TEST_TABLE_NAME, "src/test/resources/many_record.txt", "-rd=\n",
                     "-e=true", "-fd=,"});
    OptionsBuilder.checkParameters("download");

    Assert.assertTrue(Boolean.valueOf(DshipContext.INSTANCE.get(Constants.EXPONENTIAL)));
    DshipContext.INSTANCE.clear();

    OptionsBuilder.buildDownloadOption(
        new String[]{"download", TEST_TABLE_NAME, "src/test/resources/many_record.txt", "-rd=\n",
                     "-e=false", "-fd=,"});
    OptionsBuilder.checkParameters("download");

    Assert.assertFalse(Boolean.valueOf(DshipContext.INSTANCE.get(Constants.EXPONENTIAL)));

    DshipContext.INSTANCE.clear();
    OptionsBuilder.buildDownloadOption(
        new String[]{"download", TEST_TABLE_NAME, "src/test/resources/many_record.txt", "-rd=\n",
                     "-exponential=false", "-fd=,"});
    OptionsBuilder.checkParameters("download");

    Assert.assertFalse(Boolean.valueOf(DshipContext.INSTANCE.get(Constants.EXPONENTIAL)));

    DshipContext.INSTANCE.clear();
    OptionsBuilder.buildDownloadOption(
        new String[]{"download", TEST_TABLE_NAME, "src/test/resources/many_record.txt", "-rd=\n",
                     "-fd=,"});
    OptionsBuilder.checkParameters("download");

    Assert.assertNull(DshipContext.INSTANCE.get(Constants.EXPONENTIAL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDownloadExponetialNeg() throws Exception {
    OptionsBuilder.buildDownloadOption(
        new String[]{"download", TEST_TABLE_NAME, "src/test/resources/many_record.txt", "-rd=\n",
                     "-e=on", "-fd=,"});
    OptionsBuilder.checkParameters("download");
  }
}
