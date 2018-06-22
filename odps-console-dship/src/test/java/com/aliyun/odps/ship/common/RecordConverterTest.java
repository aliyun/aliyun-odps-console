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

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;


/**
 * 测试数据类型转换.<br/>
 * 目前支持的数据类型有：BIGINT\STRING\DATETIME\BOOLEAM\BOUBLE\DECIMAL
 * **/
public class RecordConverterTest {

  TimeZone gmt = TimeZone.getTimeZone("GMT+8");

  /**
   * 测试所有正确的数据类型由string数组<->record的相互转换。<br/>
   * 数据类型包括：BIGINT\STRING\DATETIME\BOOLEAM\BOUBLE\DECIMAL<br/>
   * 测试目的：<br/>
   * 1) 数据上传需要把string数组转成tunnel的一个record.<br/>
   * 2) 下载需要把rocord转换成string数组。<br/>
   * */
  @Test
  public void testNormal() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("s1", OdpsType.STRING));
    rs.addColumn(new Column("d1", OdpsType.DATETIME));
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("doub1", OdpsType.DOUBLE));
    rs.addColumn(new Column("de1", OdpsType.DOUBLE));
    String[] l = new String[] {"1", "测试string", "20130925101010", "true", "2345.1209", "2345.1"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));
    SimpleDateFormat formater = new SimpleDateFormat("yyyyMMddHHmmss");
    formater.setTimeZone(gmt);
    assertEquals("bigint not equal.", Long.valueOf(1), r.getBigint(0));
    assertEquals("string not equal.", "测试string", r.getString(1));
    assertEquals("datetime not equal.", formater.parse("20130925101010"), r.getDatetime(2));
    assertEquals("boolean not equal.", Boolean.valueOf("true"), r.getBoolean(3));
    assertEquals("double not equal.", new Double("2345.1209"), r.getDouble(4));
//    assertEquals("decimal not equal.", new BigDecimal("2345.1"), r.getDecimal(5));
    
    byte[][] l1 = cv.format(r);
    for (int i = 0; i<l1.length; i++){
      assertEquals("converter at index:"+i, l[i], new String(l1[i], "UTF-8"));
    }

  }


  /**
   * 测试布尔型，数据类型的取值 值："true", "false", "1", "0"<br/>
   * 测试目的：<br/>
   * 1） boolean型上传时，数据文件能够包括这4个值。<br/>
   * 2） 上传数据如果不在这4个值内，抛出脏数据异常，脏数据异常信息符合预期<br/>
   * 3)  下载时，对应的值为"true", "false"<br/>
   * */
  @Test
  public void testBoolean() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("b2", OdpsType.BOOLEAN));
    rs.addColumn(new Column("b3", OdpsType.BOOLEAN));
    rs.addColumn(new Column("b4", OdpsType.BOOLEAN));

    String[] l = new String[] {"true", "false", "1", "0"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);

    Record r = cv.parse(toByteArray(l));
    assertEquals("b1 not equal.", true, r.getBoolean(0));
    assertEquals("b2 not equal.", false, r.getBoolean(1));
    assertEquals("b3 not equal.", true, r.getBoolean(2));
    assertEquals("b4 not equal.", false, r.getBoolean(3));

    l = new String[] {"true", "false", "true", "false"};
    byte[][] l1 = cv.format(r);
    for (int i = 0; i<l1.length; i++){
      assertEquals("converter at index:"+i, l[i], new String(l1[i]));
    }
    
    try {
      String[] l2 = new String[] {"true", "false", "1", "a"};
      Record r2 = cv.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(),
          e.getMessage().indexOf("ERROR: format error - :4, BOOLEAN:'a'") >= 0);
    }
  }

  @Test
  public void testDoubleWithExponential() throws Exception {
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("d1", OdpsType.DOUBLE));
    rs.addColumn(new Column("d2", OdpsType.DOUBLE));
    rs.addColumn(new Column("d3", OdpsType.DOUBLE));
    rs.addColumn(new Column("d4", OdpsType.DOUBLE));
    rs.addColumn(new Column("d5", OdpsType.DOUBLE));
    rs.addColumn(new Column("d6", OdpsType.DOUBLE));
    rs.addColumn(new Column("d7", OdpsType.DOUBLE));

    String[] l =
        new String[] {"211.234567", "Infinity", "-Infinity", "12345678.1234567",
                      "1.23456781234567E7", "1.2345E2", "1.2345E10"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", Constants.REMOTE_CHARSET, null, true);

    Record r = cv.parse(toByteArray(l));
    assertEquals("d1 not equal.", "211.234567", r.getDouble(0).toString());
    assertEquals("d2 not equal.", new Double(Double.POSITIVE_INFINITY).toString(), r.getDouble(1)
        .toString());
    assertEquals("d3 not equal.", new Double(Double.NEGATIVE_INFINITY).toString(), r.getDouble(2)
        .toString());
    assertEquals("d3 not equal d4.", r.getDouble(3), r.getDouble(4));

    l =
        new String[] {"211.234567", "Infinity", "-Infinity", "1.23456781234567E7",
                      "1.23456781234567E7", "123.45", "1.2345E10"};
    byte[][] l1 = cv.format(r);
    for (int i = 0; i<l1.length; i++){
      assertEquals("converter at index:"+i, l[i], new String(l1[i]));
    }

    try {
      String[] l2 = new String[] {"12345678.1234567", "1.23456781234567E7", "1", "a", "b" , "1", "1"};
      Record r2 = cv.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(),
                 e.getMessage().indexOf("ERROR: format error - :4, DOUBLE:'a'") >= 0);
    }

  }

  /**
   * 测试double类型取值 取值范围：数字、科学计数法、无穷大（"Infinity"）,无穷小（"-Infinity"）<br/>
   * 测试目的：<br/>
   * 1) double类型的值可以在此取值范围内<br/>
   * 2）不在此范围内值，抛出脏数据异常，异常信息符合预期<br/>
   * 3) 下载值和上传值一致。<br/>
   * */
  @Test
  public void testDouble() throws Exception {
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("d1", OdpsType.DOUBLE));
    rs.addColumn(new Column("d2", OdpsType.DOUBLE));
    rs.addColumn(new Column("d3", OdpsType.DOUBLE));
    rs.addColumn(new Column("d4", OdpsType.DOUBLE));
    rs.addColumn(new Column("d5", OdpsType.DOUBLE));
    rs.addColumn(new Column("d6", OdpsType.DOUBLE));
    rs.addColumn(new Column("d7", OdpsType.DOUBLE));

    String[] l =
        new String[] {"211.234567", "Infinity", "-Infinity", "12345678.1234567",
            "1.23456781234567E7", "1.2345E2", "1.2345E10"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);

    Record r = cv.parse(toByteArray(l));
    assertEquals("d1 not equal.", "211.234567", r.getDouble(0).toString());
    assertEquals("d2 not equal.", new Double(Double.POSITIVE_INFINITY).toString(), r.getDouble(1)
        .toString());
    assertEquals("d3 not equal.", new Double(Double.NEGATIVE_INFINITY).toString(), r.getDouble(2)
        .toString());
    assertEquals("d3 not equal d4.", r.getDouble(3), r.getDouble(4));
    
    l =
    new String[] {"211.234567", "Infinity", "-Infinity", "12345678.1234567",
        "12345678.1234567", "123.45", "12345000000"};
    byte[][] l1 = cv.format(r);
    for (int i = 0; i<l1.length; i++){
      assertEquals("converter at index:"+i, l[i], new String(l1[i]));
    }
    
    try {
      String[] l2 = new String[] {"12345678.1234567", "1.23456781234567E7", "1", "a", "b" , "1", "1"};
      Record r2 = cv.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(),
          e.getMessage().indexOf("ERROR: format error - :4, DOUBLE:'a'") >= 0);
    }

  }

  /**
   * 测试字符类型<br/>
   * 测试目的：<br/>
   * 1) 字符串长度小于8M字符，正常<br/>
   * 2）大于8M字符串，抛出脏数据异常，异常符合预期<br/>
   * 3) 下载字符串一致<br/>
   * */
  @Test
  public void testString() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("s1", OdpsType.STRING));

    String[] l = new String[] {"aaa"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));
    assertEquals("b1 not equal.", "aaa", r.getString(0));
    
    byte[][] l1 = cv.format(r);
    for (int i = 0; i<l1.length; i++){
      assertEquals("converter at index:"+i, l[i], new String(l1[i]));
    }
    
    try {
      String[] l2 = new String[] {""};
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 8 * 1024 * 1024 + 1; i++) {
        sb.append('a');
      }
      l2[0] = sb.toString();

      Record r2 = cv.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().contains("ERROR: format error - :1, STRING:'aaaaaaaaaaaaaaaaa...'"));
    }
  }

  /**
   * 测试日期类型，及pattern出错的情况<br/>
   * 测试目的：<br/>
   * 1） 指定pattern正确，数据正常转换<br/>
   * 2） 指定pattern和数据不一致，抛出脏数据异常，异常符合预期<br/>
   * 3) 上传和下载数据一致<br/>
   * */
  @Test
  public void testDatetime() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("d1", OdpsType.DATETIME));

    String[] l = new String[] {"20131010101010"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    SimpleDateFormat formater = new SimpleDateFormat("yyyyMMddHHmmss");
    formater.setTimeZone(gmt);
    Record r = cv.parse(toByteArray(l));
    assertEquals("d1 not equal.", formater.parse("20131010101010"), r.getDatetime(0));

    byte[][] l1 = cv.format(r);
    for (int i = 0; i<l1.length; i++){
      assertEquals("converter at index:"+i, l[i], new String(l1[i]));
    }
    
    l = new String[] {"20131010101010"};
    cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", "GMT+6");
    formater = new SimpleDateFormat("yyyyMMddHHmmss");
    formater.setTimeZone(TimeZone.getTimeZone("GMT+6"));
    r = cv.parse(toByteArray(l));
    assertEquals("d1 not equal.", formater.parse("20131010101010"), r.getDatetime(0));
    l1 = cv.format(r);
    for (int i = 0; i<l1.length; i++){
      assertEquals("converter at index:"+i, l[i], new String(l1[i]));
    }
    
    try {
      String[] l2 = new String[] {"20131010101080"};
      Record r2 = cv.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().indexOf("ERROR: format error - :1, DATETIME:'20131010101080'") >= 0);
    }

    l = new String[] {"2013-09-25"};
    cv = new RecordConverter(rs, "NULL", "yyyy-MM-dd", null);
    formater = new SimpleDateFormat("yyyy-MM-dd");
    formater.setTimeZone(gmt);
    r = cv.parse(toByteArray(l));
    assertEquals("d1 not equal.", formater.parse("2013-09-25"), r.getDatetime(0));
  }

//  /**
//   * 测试decimal类型<br/>
//   * 测试目的：<br/>
//   * 1） 上传下载数据一致<br/>
//   * 2） 脏数据出错，出错符合预期<br/>
//   * */
//  @Test
//  public void testDecimal() throws Exception {
//    
//    TableSchema rs = new TableSchema();
//    rs.addColumn(new Column("de1", OdpsType.DECIMAL));
//
//    String[] l = new String[] {"123.12"};
//    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
//    Record r = cv.parse(toByteArray(l), 1);
//    assertEquals("d1 not equal.", "123.12", r.getDecimal(0).toString());
//
//    String[] l1 = cv.format(r);
//    for (int i = 0; i<l1.length; i++){
//      assertEquals("converter at index:"+i, l[i], l1[i]);
//    }
//    
//    try {
//      String[] l2 = new String[] {"12345678.1234567a"};
//      Record r2 = cv.parse(l2, 1);
//      fail("need fail");
//    } catch (Exception e) {
//      assertTrue(e.getMessage(),
//          e.getMessage().indexOf("ERROR: format error - line 1:1, ODPS_DECIMAL:'12345678.1234567a'") >= 0);
//    }
//  }

  /**
   * 测试Null indicator值<br/>
   * 测试目的：<br/>
   * 1）测试所有数据类型的空值上传和下载一致<br/>
   * */
  @Test
  public void testNull() throws Exception {
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("s1", OdpsType.STRING));
    rs.addColumn(new Column("d1", OdpsType.DATETIME));
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("doub1", OdpsType.DOUBLE));
    rs.addColumn(new Column("de1", OdpsType.DOUBLE));
    rs.addColumn(new Column("n1", OdpsType.STRING));

    String[] l = new String[] {"NULL", "NULL", "NULL", "NULL", "NULL", "NULL", ""};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));

    assertNull("bigint not null.", r.getBigint(0));
    assertNull("string not null.", r.getBigint(1));
    assertNull("datetime not null.", r.getBigint(2));
    assertNull("boolean not null.", r.getBigint(3));
    assertNull("double not null.", r.getBigint(4));
    assertNull("decimal not null.", r.getBigint(5));
    assertEquals("n1 not equal.", "", r.getString(6));
    
    byte[][] l1 = cv.format(r);
    for (int i = 0; i<l1.length; i++){
      assertEquals("converter at index:"+i, l[i], new String(l1[i]));
    }
  }


  @Test
  public void testNullReuse() throws Exception {
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));

    String[] l0 = new String[] {"2"};
    String[] l1 = new String[] {"NULL"};
    String[] l2 = new String[] {"1"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l0));
    assertEquals(r.getBigint(0), new Long(2L));
    r = cv.parse(toByteArray(l1));
    assertEquals(r.getBigint(0), null);
    r = cv.parse(toByteArray(l2));
    assertEquals(r.getBigint(0), new Long(1L));
  }

  @Test(expected = ParseException.class)
  public void testNullSet() throws Exception {
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));

    String[] l = new String[] {"2", "NULL"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));
  }
  
  /**
   * 测试bigint的最大值，最小值的边界, 最大值：9223372036854775807， 最小值：-9223372036854775807
   * */
  @Test
  public void testBigint()throws Exception{
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("i2", OdpsType.BIGINT));
    
    String[] l = new String[] {"9223372036854775807", "-9223372036854775807"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));
    assertEquals("max value", Long.valueOf("9223372036854775807"), r.getBigint(0));
    assertEquals("min value", Long.valueOf("-9223372036854775807"), r.getBigint(1));
    
    try {
      l = new String[] {"9223372036854775807", "-9223372036854775808"};
      r = cv.parse(toByteArray(l));
    } catch (Exception e) {
      assertTrue("big int min value", e.getMessage().startsWith("ERROR: format error - :2, BIGINT:'-9223372036854775808'"));
    }
    
    try {
      l = new String[] {"9223372036854775808", "-9223372036854775807"};
      r = cv.parse(toByteArray(l));
    } catch (Exception e) {
      assertTrue("big int min value", e.getMessage().startsWith("ERROR: format error - :1, BIGINT:'9223372036854775808'"));
    }
    
  }

  private byte[][] toByteArray(String[] s, String charset)  throws UnsupportedEncodingException {
    byte[][] b = new byte[s.length][];

    for (int i = 0; i < s.length; ++i) {
      b[i] = s[i].getBytes(charset);
    }
    return b;
  }

  private byte[][] toByteArray(String[] s)  throws UnsupportedEncodingException {
    return toByteArray(s, "UTF-8");
  }

  /**
   * 测试字符类型<br/>
   * 测试目的：<br/>
   * raw data数据没有使用指定的charset编码<br/>
   * */
  @Test
  public void testFormateRawData() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("s1", OdpsType.STRING));

    String[] l = new String[] {"测试字段"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));
    assertEquals("b1 not equal.", l[0], r.getString(0));
    
    //default format
    byte[][] l1 = cv.format(r);
    assertEquals("converter at index0:", l[0], new String(l1[0], "UTF-8"));

    //format with charset gdb
    RecordConverter cv2 = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null, "gbk");
    r = cv2.parse(toByteArray(l, "gbk"));
    byte[][] l2 = cv2.format(r);
    //string has encoded by gbk charset
    assertEquals("converter at index0:", l[0], new String(l2[0], "gbk"));

    //format without encoding
    RecordConverter cv3 = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null, Constants.IGNORE_CHARSET);
    byte[][] l3 = cv3.format(r);
    // string is not encoded by gdk charset, use default instead.
    assertEquals("converter at index0:", l[0], new String(l3[0], "UTF-8"));

    //format without encoding
    RecordConverter cv4 = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null, null);
    byte[][] l4 = cv4.format(r);
    // string is not encoded by gdk charset, use default instead.
    assertEquals("converter at index0:", l[0], new String(l4[0], "UTF-8"));
  }

}
