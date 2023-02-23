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

import static org.junit.Assert.assertArrayEquals;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.Charset;

/**
 * 测试系统默认 charset 非 utf-8 时 RecordConverter 的行为
 * 这个测试类应该在一个单独的进程中，避免影响其他测试
 */
public class RecordConverterGBKTest {

  static String originalCharset;

  @BeforeClass
  public static void setToGBK() throws Exception {
    originalCharset = System.getProperty("file.encoding");
    setSystemDefaultCharset("gbk");
  }

  @AfterClass
  public static void revertSystemCharset() throws Exception {
    setSystemDefaultCharset(originalCharset);
  }

  @Test
  public void testDefaultCharsetGBK() throws Exception {
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("s1", OdpsType.STRING));

    Record r = new ArrayRecord(rs.getColumns().toArray(new Column[0]));
    r.setString(0, "中文".getBytes("UTF-8"));

    RecordConverter rc = new RecordConverter(
            rs, "NULL", "yyyyMMddHHmmss", null,
            "gbk", false, false );
    byte[][] b = rc.format(r);
    assertArrayEquals("converter at index0:", b[0], "中文".getBytes("gbk"));
  }

  public static void setSystemDefaultCharset(String s)
      throws NoSuchFieldException, IllegalAccessException {
    System.setProperty("file.encoding", s);
    Field charset = Charset.class.getDeclaredField("defaultCharset");
    charset.setAccessible(true);
    charset.set(null, null);
  }

}
