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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.google.common.collect.Lists;


/**
 * 测试数据类型转换.<br/> 目前支持的数据类型有：BIGINT\STRING\DATETIME\BOOLEAM\BOUBLE\DECIMAL
 **/
public class RecordConverterTest {

  TimeZone gmt = TimeZone.getTimeZone("GMT+8");

  /**
   * 测试所有正确的数据类型由string数组<->record的相互转换。<br/> 数据类型包括：BIGINT\STRING\DATETIME\BOOLEAM\BOUBLE\DECIMAL<br/>
   * 测试目的：<br/> 1) 数据上传需要把string数组转成tunnel的一个record.<br/> 2) 下载需要把rocord转换成string数组。<br/>
   */
  @Test
  public void testNormal() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("s1", OdpsType.STRING));
    rs.addColumn(new Column("d1", OdpsType.DATETIME));
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("doub1", OdpsType.DOUBLE));
    rs.addColumn(new Column("de1", OdpsType.DOUBLE));
    String[] l = new String[]{"1", "测试string", "20130925101010", "true", "2345.1209", "2345.1"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));
    SimpleDateFormat formater = new SimpleDateFormat("yyyyMMddHHmmss");
//    formater.setTimeZone(gmt);
    assertEquals("bigint not equal.", Long.valueOf(1), r.getBigint(0));
    assertEquals("string not equal.", "测试string", r.getString(1));
    assertEquals("datetime not equal.", formater.parse("20130925101010"), r.getDatetime(2));
    assertEquals("boolean not equal.", Boolean.valueOf("true"), r.getBoolean(3));
    assertEquals("double not equal.", new Double("2345.1209"), r.getDouble(4));
//    assertEquals("decimal not equal.", new BigDecimal("2345.1"), r.getDecimal(5));

    byte[][] l1 = cv.format(r);
    for (int i = 0; i < l1.length; i++) {
      assertEquals("converter at index:" + i, l[i], new String(l1[i], "UTF-8"));
    }

  }


  /**
   * 测试布尔型，数据类型的取值 值："true", "false", "1", "0"<br/> 测试目的：<br/> 1） boolean型上传时，数据文件能够包括这4个值。<br/> 2）
   * 上传数据如果不在这4个值内，抛出脏数据异常，脏数据异常信息符合预期<br/> 3)  下载时，对应的值为"true", "false"<br/>
   */
  @Test
  public void testBoolean() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("b1", OdpsType.BOOLEAN));
    rs.addColumn(new Column("b2", OdpsType.BOOLEAN));
    rs.addColumn(new Column("b3", OdpsType.BOOLEAN));
    rs.addColumn(new Column("b4", OdpsType.BOOLEAN));

    String[] l = new String[]{"true", "false", "1", "0"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);

    Record r = cv.parse(toByteArray(l));
    assertEquals("b1 not equal.", true, r.getBoolean(0));
    assertEquals("b2 not equal.", false, r.getBoolean(1));
    assertEquals("b3 not equal.", true, r.getBoolean(2));
    assertEquals("b4 not equal.", false, r.getBoolean(3));

    l = new String[]{"true", "false", "true", "false"};
    byte[][] l1 = cv.format(r);
    for (int i = 0; i < l1.length; i++) {
      assertEquals("converter at index:" + i, l[i], new String(l1[i]));
    }

    try {
      String[] l2 = new String[]{"true", "false", "1", "a"};
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
        new String[]{"211.234567", "Infinity", "-Infinity", "12345678.1234567",
                     "1.23456781234567E7", "1.2345E2", "1.2345E10"};
    RecordConverter
        cv =
        new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", Constants.REMOTE_CHARSET, null, true);

    Record r = cv.parse(toByteArray(l));
    assertEquals("d1 not equal.", "211.234567", r.getDouble(0).toString());
    assertEquals("d2 not equal.", new Double(Double.POSITIVE_INFINITY).toString(), r.getDouble(1)
        .toString());
    assertEquals("d3 not equal.", new Double(Double.NEGATIVE_INFINITY).toString(), r.getDouble(2)
        .toString());
    assertEquals("d3 not equal d4.", r.getDouble(3), r.getDouble(4));

    l =
        new String[]{"211.234567", "Infinity", "-Infinity", "1.23456781234567E7",
                     "1.23456781234567E7", "123.45", "1.2345E10"};
    byte[][] l1 = cv.format(r);
    for (int i = 0; i < l1.length; i++) {
      assertEquals("converter at index:" + i, l[i], new String(l1[i]));
    }

    try {
      String[] l2 = new String[]{"12345678.1234567", "1.23456781234567E7", "1", "a", "b", "1", "1"};
      Record r2 = cv.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(),
                 e.getMessage().indexOf("ERROR: format error - :4, DOUBLE:'a'") >= 0);
    }

  }

  /**
   * 测试double类型取值 取值范围：数字、科学计数法、无穷大（"Infinity"）,无穷小（"-Infinity"）<br/> 测试目的：<br/> 1)
   * double类型的值可以在此取值范围内<br/> 2）不在此范围内值，抛出脏数据异常，异常信息符合预期<br/> 3) 下载值和上传值一致。<br/>
   */
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
        new String[]{"211.234567", "Infinity", "-Infinity", "12345678.1234567",
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
        new String[]{"211.234567", "Infinity", "-Infinity", "12345678.1234567",
                     "12345678.1234567", "123.45", "12345000000"};
    byte[][] l1 = cv.format(r);
    for (int i = 0; i < l1.length; i++) {
      assertEquals("converter at index:" + i, l[i], new String(l1[i]));
    }

    try {
      String[] l2 = new String[]{"12345678.1234567", "1.23456781234567E7", "1", "a", "b", "1", "1"};
      Record r2 = cv.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(),
                 e.getMessage().indexOf("ERROR: format error - :4, DOUBLE:'a'") >= 0);
    }

  }

  /**
   * 测试日期类型，及pattern出错的情况<br/> 测试目的：<br/> 1） 指定pattern正确，数据正常转换<br/> 2）
   * 指定pattern和数据不一致，抛出脏数据异常，异常符合预期<br/> 3) 上传和下载数据一致<br/>
   */
  @Test
  public void testDatetime() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("d1", OdpsType.DATETIME));

    String[] l = new String[]{"20131010101010"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    SimpleDateFormat formater = new SimpleDateFormat("yyyyMMddHHmmss");
//    formater.setTimeZone(gmt);
    Record r = cv.parse(toByteArray(l));
    assertEquals("d1 not equal.", formater.parse("20131010101010"), r.getDatetime(0));

    byte[][] l1 = cv.format(r);
    for (int i = 0; i < l1.length; i++) {
      assertEquals("converter at index:" + i, l[i], new String(l1[i]));
    }

    l = new String[]{"20131010101010"};
    cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", "GMT+6");
    formater = new SimpleDateFormat("yyyyMMddHHmmss");
    formater.setTimeZone(TimeZone.getTimeZone("GMT+6"));
    r = cv.parse(toByteArray(l));
    assertEquals("d1 not equal.", formater.parse("20131010101010"), r.getDatetime(0));
    l1 = cv.format(r);
    for (int i = 0; i < l1.length; i++) {
      assertEquals("converter at index:" + i, l[i], new String(l1[i]));
    }

    try {
      String[] l2 = new String[]{"20131010101080"};
      Record r2 = cv.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().indexOf("ERROR: format error - :1, DATETIME:'20131010101080'") >= 0);
    }

    l = new String[]{"2013-09-25"};
    cv = new RecordConverter(rs, "NULL", "yyyy-MM-dd", null);
    formater = new SimpleDateFormat("yyyy-MM-dd");
//    formater.setTimeZone(gmt);
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
   * 测试Null indicator值<br/> 测试目的：<br/> 1）测试所有数据类型的空值上传和下载一致<br/>
   */
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

    String[] l = new String[]{"NULL", "NULL", "NULL", "NULL", "NULL", "NULL", ""};
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
    for (int i = 0; i < l1.length; i++) {
      assertEquals("converter at index:" + i, l[i], new String(l1[i]));
    }
  }


  @Test
  public void testNullReuse() throws Exception {
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));

    String[] l0 = new String[]{"2"};
    String[] l1 = new String[]{"NULL"};
    String[] l2 = new String[]{"1"};
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

    String[] l = new String[]{"2", "NULL"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));
  }

  /**
   * 测试bigint的最大值，最小值的边界, 最大值：9223372036854775807， 最小值：-9223372036854775807
   */
  @Test
  public void testBigint() throws Exception {
    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("i1", OdpsType.BIGINT));
    rs.addColumn(new Column("i2", OdpsType.BIGINT));

    String[] l = new String[]{"9223372036854775807", "-9223372036854775807"};
    RecordConverter cv = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null);
    Record r = cv.parse(toByteArray(l));
    assertEquals("max value", Long.valueOf("9223372036854775807"), r.getBigint(0));
    assertEquals("min value", Long.valueOf("-9223372036854775807"), r.getBigint(1));

    try {
      l = new String[]{"9223372036854775807", "-9223372036854775808"};
      r = cv.parse(toByteArray(l));
    } catch (Exception e) {
      assertTrue("big int min value", e.getMessage()
          .startsWith("ERROR: format error - :2, BIGINT:'-9223372036854775808'"));
    }

    try {
      l = new String[]{"9223372036854775808", "-9223372036854775807"};
      r = cv.parse(toByteArray(l));
    } catch (Exception e) {
      assertTrue("big int min value", e.getMessage()
          .startsWith("ERROR: format error - :1, BIGINT:'9223372036854775808'"));
    }

  }

  private byte[][] toByteArray(String[] s, String charset) throws UnsupportedEncodingException {
    byte[][] b = new byte[s.length][];

    for (int i = 0; i < s.length; ++i) {
      b[i] = s[i].getBytes(charset);
    }
    return b;
  }

  private byte[][] toByteArray(String[] s) throws UnsupportedEncodingException {
    return toByteArray(s, "UTF-8");
  }

  /**
   * 测试字符类型<br/> 测试目的：<br/> raw data数据没有使用指定的charset编码<br/>
   */
  @Test
  public void testFormateRawData() throws Exception {

    TableSchema rs = new TableSchema();
    rs.addColumn(new Column("s1", OdpsType.STRING));

    String[] l = new String[]{"测试字段"};
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
    RecordConverter
        cv3 =
        new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null, Constants.IGNORE_CHARSET);
    byte[][] l3 = cv3.format(r);
    // string is not encoded by gdk charset, use default instead.
    assertEquals("converter at index0:", l[0], new String(l3[0], "UTF-8"));

    //format without encoding
    RecordConverter cv4 = new RecordConverter(rs, "NULL", "yyyyMMddHHmmss", null, null);
    byte[][] l4 = cv4.format(r);
    // string is not encoded by gdk charset, use default instead.
    assertEquals("converter at index0:", l[0], new String(l4[0], "UTF-8"));
  }

  /**
   * 测试只包含基础类型的Array<br/> 基础类型包括BIGINT, STRING<br/>
   */
  @Test
  public void testArray() throws UnsupportedEncodingException, ParseException {
    TableSchema schema = new TableSchema();
    TypeInfo typeInfo1 = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT);
    TypeInfo typeInfo2 = TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING);
    Column c1 = new Column("c1", typeInfo1);
    Column c2 = new Column("c2", typeInfo2);
    schema.addColumn(c1);
    schema.addColumn(c2);
    RecordConverter
        converter =
        new RecordConverter(schema, "null", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);
    Record record1 = new ArrayRecord(new Column[]{c1, c2});
    List<Long> list1 = Lists.newArrayList(1l, 2l);
    List<String> list2 = new ArrayList<>();
    list2.add(null);
    list2.add("");
    list2.add("NULL");
    record1.set(0, list1);
    record1.set(1, list2);
    byte[][] bytes = converter.format(record1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    Record record2 = converter.parse(bytes);
    assertEquals(record1.get(0), record2.get(0));
    assertEquals(record1.get(1), record2.get(1));
  }

  /**
   * 测试嵌套Array的Array<br/> 基础类型包括：DOUBLE, STRING<br/>
   */
  @Test
  public void testArrayWithArray() throws UnsupportedEncodingException, ParseException {
    TableSchema schema = new TableSchema();
    TypeInfo
        typeInfo1 =
        TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.DOUBLE));
    TypeInfo
        typeInfo2 =
        TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.STRING));
    Column c1 = new Column("c1", typeInfo1);
    Column c2 = new Column("c2", typeInfo2);
    schema.addColumn(c1);
    schema.addColumn(c2);
    RecordConverter
        converter =
        new RecordConverter(schema, "null", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);
    Record record1 = new ArrayRecord(new Column[]{c1, c2});
    List<Double> list1 = Lists.newArrayList(1d, 2d);
    List<String> list2 = Lists.newArrayList(null, "test", "NULL", "");

    List<List<Double>> r1 = new ArrayList<>();
    r1.add(list1);
    List<List<String>> r2 = new ArrayList<>();
    r2.add(list2);
    r2.add(null);
    r2.add(new ArrayList<>());
    record1.set(0, r1);
    record1.set(1, r2);

    byte[][] bytes = converter.format(record1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    Record record2 = converter.parse(bytes);
    assertEquals(record1.get(0), record2.get(0));
    assertEquals(record1.get(1), record2.get(1));
  }

  /**
   * 测试嵌套Map的Array<br/> 基础类型包括 STRING, DATETIME<br/>
   */
  @Test
  public void testArrayWithMap() throws Exception {
    TableSchema schema = new TableSchema();
    TypeInfo
        typeInfo =
        TypeInfoFactory.getArrayTypeInfo(
            TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.DATETIME));
    schema.addColumn(new Column("c1", typeInfo));
    RecordConverter
        converter =
        new RecordConverter(schema, "null", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);

    Record r1 = new ArrayRecord(new Column[]{new Column("c1", typeInfo)});
    List<Map<String, Date>> data = new ArrayList<>();
    data.add(Collections.singletonMap("foo", new Date()));
    data.add(Collections.singletonMap("bar", new Date()));
    data.add(null);
    data.add(Collections.EMPTY_MAP);
    data.add(Collections.singletonMap(null, new Date()));
    data.add(Collections.singletonMap("NULL", null));
    data.add(Collections.singletonMap("", null));
    System.out.println(data.toString());
    r1.set(0, data);

    byte[][] bytes = converter.format(r1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    Record r2 = converter.parse(bytes);
    List<Map<String, Date>> parsed = (List<Map<String, Date>>) r2.get(0);
    System.out.println(parsed.toString());
    Assert.assertEquals(data.toString(), parsed.toString());
  }

  /**
   * 测试嵌套Struct的Array<br/> 基础类型包括 STRING, DATETIME<br/>
   */
  @Test
  public void testArrayWithStruct() throws UnsupportedEncodingException, ParseException {
    TableSchema schema = new TableSchema();
    StructTypeInfo
        structTypeInfo =
        TypeInfoFactory.getStructTypeInfo(Lists.newArrayList("f1", "f2"), Lists
            .newArrayList(TypeInfoFactory.STRING, TypeInfoFactory.DATETIME));
    TypeInfo typeInfo1 = TypeInfoFactory.getArrayTypeInfo(structTypeInfo);
    Column c1 = new Column("c1", typeInfo1);
    schema.addColumn(c1);
    RecordConverter
        converter =
        new RecordConverter(schema, "null", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);
    Record record1 = new ArrayRecord(new Column[]{c1});

    Struct struct1 = new SimpleStruct(structTypeInfo, Lists.newArrayList("", new Date()));
    Struct struct2 = new SimpleStruct(structTypeInfo, Lists.newArrayList(null, new Date()));
    Struct struct3 = new SimpleStruct(structTypeInfo, Lists.newArrayList("NULL", null));

    List<Struct> list = Lists.newArrayList(struct1, struct2, struct3, null);
    record1.set(0, list);
    System.out.println(record1.get(0));

    byte[][] bytes = converter.format(record1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    Record record2 = converter.parse(bytes);
    System.out.println(record2.get(0));
    assertEquals(record1.get(0).toString(), record2.get(0).toString());
  }

  /**
   * 测试嵌套Array的Map, Array作为key和value的情况<br/> 基础类型包括：STRING, FLOAT<br/>
   */
  @Test
  public void testMapWithArray() throws Exception {
    TableSchema schema = new TableSchema();
    TypeInfo
        typeInfo1 =
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING,
                                       TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.FLOAT));
    TypeInfo
        typeInfo2 =
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.FLOAT),
                                       TypeInfoFactory.STRING);

    Column c1 = new Column("c1", typeInfo1);
    Column c2 = new Column("c2", typeInfo2);
    schema.addColumn(c1);
    schema.addColumn(c2);
    RecordConverter
        converter =
        new RecordConverter(schema, "NULL", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);
    Record record1 = new ArrayRecord(new Column[]{c1, c2});

    Map<String, List<Float>> map1 = new HashMap<>();
    Map<List<Float>, String> map2 = new HashMap<>();

    List<Float> list = Lists.newArrayList(1f, 2.4f, 3.1f);
    map1.put(null, list);
    map1.put(" ", new ArrayList<>());
    map1.put("null", null);

    map2.put(null, "null");
    map2.put(list, " ");
    map2.put(new ArrayList<>(), null);

    record1.set(0, map1);
    record1.set(1, map2);
    System.out.println(record1.get(0));
    System.out.println(record1.get(1));

    byte[][] bytes = converter.format(record1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    Record record2 = converter.parse(bytes);
    System.out.println(record2.get(0));
    System.out.println(record2.get(1));
    assertEquals(record1.get(0), record2.get(0));
    assertEquals(record1.get(1), record2.get(1));
  }


  /**
   * 测试只包含基础类型的Map 和 嵌套Map的Map<br/> 基础类型包括：STRING, DATE, BOOLEAN<br/> 遗留问题：时间不匹配<br/>
   */
  @Test
  public void testMapWithMap() throws Exception {
    TableSchema schema = new TableSchema();
    TypeInfo
        typeInfo1 =
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, TypeInfoFactory.DATE);
    TypeInfo typeInfo2 = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.BOOLEAN, typeInfo1);
    Column c1 = new Column("c1", typeInfo1);
    Column c2 = new Column("c2", typeInfo2);
    schema.addColumn(c1);
    schema.addColumn(c2);
    RecordConverter
        converter =
        new RecordConverter(schema, "null", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);
    Record record1 = new ArrayRecord(new Column[]{c1, c2});

    Map<String, Date> map1 = new HashMap<>();
    map1.put(null, new Date());
    map1.put("test", new Date());
    map1.put("NULL", null);
    map1.put(" ", null);

    Map<Boolean, Map<String, Date>> map2 = new HashMap<>();
    map2.put(true, map1);
    map2.put(false, null);
    map2.put(null, new HashMap<>());

    record1.set(0, map1);
    record1.set(1, map2);
    System.out.println(record1.get(0));
    System.out.println(record1.get(1));

    byte[][] bytes = converter.format(record1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();
    Record record2 = converter.parse(bytes);
    System.out.println(record2.get(0));
    System.out.println(record2.get(1));
    assertEquals(record1.get(0), record2.get(0));
    assertEquals(record1.get(1), record2.get(1));
  }

  /**
   * 测试嵌套Struct的Map<br/> 基础类型包括：STRING, DATETIME<br/>
   */
  @Test
  public void testMapWithStruct() throws Exception {
    TableSchema schema = new TableSchema();
    StructTypeInfo
        structTypeInfo =
        TypeInfoFactory.getStructTypeInfo(Lists.newArrayList("f1", "f2"), Lists
            .newArrayList(TypeInfoFactory.STRING, TypeInfoFactory.DATETIME));
    TypeInfo typeInfo1 = TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.STRING, structTypeInfo);
    Column c1 = new Column("c1", typeInfo1);
    schema.addColumn(c1);

    RecordConverter
        converter =
        new RecordConverter(schema, "NULL", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);
    Record record1 = new ArrayRecord(new Column[]{c1});

    Map<String, Struct> map1 = new HashMap<>();
    Struct struct1 = new SimpleStruct(structTypeInfo, Lists.newArrayList("test", new Date()));
    Struct struct2 = new SimpleStruct(structTypeInfo, Lists.newArrayList(null, new Date()));
    Struct struct3 = new SimpleStruct(structTypeInfo, Lists.newArrayList("null", null));

    map1.put("test", struct1);
    map1.put("null", struct2);
    map1.put(null, struct3);
    map1.put(" ", null);

    record1.set(0, map1);
    System.out.println(record1.get(0));

    byte[][] bytes = converter.format(record1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    Record record2 = converter.parse(bytes);
    System.out.println(record2.get(0));
    assertEquals(record1.get(0).toString(), record2.get(0).toString());
  }

  /**
   * 测试嵌套Array, Map, Struct的Struct<br/> 基础类型包括： DATETIME, BOOLEAN, BIGINT, DOUBLE, FLOAT<br/>
   */
  @Test
  public void testStruct() throws Exception {
    TableSchema schema = new TableSchema();
    StructTypeInfo
        structTypeInfo =
        TypeInfoFactory.getStructTypeInfo(Lists.newArrayList("f1", "f2", "f3"),
                                          Lists.newArrayList(TypeInfoFactory.STRING,
                                                             TypeInfoFactory.DATETIME,
                                                             TypeInfoFactory.BOOLEAN));
    TypeInfo typeInfo1 = TypeInfoFactory.getStructTypeInfo(
        Lists.newArrayList("f1", "f2", "f3"),
        Lists.newArrayList(
            TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT),
            TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.DOUBLE, TypeInfoFactory.FLOAT),
            structTypeInfo));

    Column c1 = new Column("c1", typeInfo1);
    Column c2 = new Column("c2", typeInfo1);
    Column c3 = new Column("c3", typeInfo1);
    schema.addColumn(c1);
    schema.addColumn(c2);
    schema.addColumn(c3);

    RecordConverter
        converter =
        new RecordConverter(schema, "NULL", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);
    Record record1 = new ArrayRecord(new Column[]{c1, c2, c3});

    List list = Lists.newArrayList(1L, 2L, 3L);
    Map<Double, Float> map = new HashMap<>();
    map.put(1.1d, 1.2f);
    map.put(2.2d, 2.3f);
    map.put(3.3d, 3.4f);

    Struct struct1 = new SimpleStruct(structTypeInfo, Lists.newArrayList(" ", new Date(), true));
    Struct struct2 = new SimpleStruct(structTypeInfo, Lists.newArrayList(null, new Date(), false));
    Struct struct3 = new SimpleStruct(structTypeInfo, Lists.newArrayList("null", null, false));

    Struct
        structC1 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(list, map, struct1));
    Struct
        structC2 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(null, map, struct2));
    Struct
        structC3 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(list, null, struct3));
    record1.set(0, structC1);
    record1.set(1, structC2);
    record1.set(2, structC3);

    System.out.println(record1.get(0));
    System.out.println(record1.get(1));
    System.out.println(record1.get(2));

    byte[][] bytes = converter.format(record1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    Record record2 = converter.parse(bytes);
    System.out.println(record2.get(0));
    System.out.println(record2.get(1));
    System.out.println(record2.get(2));
    assertEquals(record1.get(0).toString(), record2.get(0).toString());
    assertEquals(record1.get(1).toString(), record2.get(1).toString());
    assertEquals(record1.get(2).toString(), record2.get(2).toString());
  }

  /**
   * 测试嵌套时间类型的Struct<br/> 时间类型包括：DATE, DATETIME, TIMESTAMP<br/> 遗留问题：DATE类型时间不匹配
   */
  @Test
  public void testStructDatetime() throws Exception {
    TableSchema schema = new TableSchema();
    StructTypeInfo
        structTypeInfo =
        TypeInfoFactory.getStructTypeInfo(Lists.newArrayList("f1", "f2", "f3"),
                                          Lists.newArrayList(TypeInfoFactory.DATE,
                                                             TypeInfoFactory.DATETIME,
                                                             TypeInfoFactory.TIMESTAMP));
    TypeInfo typeInfo1 = TypeInfoFactory.getStructTypeInfo(
        Lists.newArrayList("f11", "f22", "f33"),
        Lists.newArrayList(
            TypeInfoFactory.getArrayTypeInfo(TypeInfoFactory.BIGINT),
            TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.DOUBLE, TypeInfoFactory.FLOAT),
            structTypeInfo));

    Column c1 = new Column("c1", typeInfo1);
    Column c2 = new Column("c2", typeInfo1);
    Column c3 = new Column("c3", typeInfo1);
    Column c4 = new Column("c4", typeInfo1);
    schema.addColumn(c1);
    schema.addColumn(c2);
    schema.addColumn(c3);
    schema.addColumn(c4);

    RecordConverter
        converter =
        new RecordConverter(schema, "NULL", "yyyy-MM-dd HH:mm:ss", null, Constants.IGNORE_CHARSET);
    Record record1 = new ArrayRecord(new Column[]{c1, c2, c3, c4});

    List list = Lists.newArrayList(1L, 2L, 3L);
    Map<Double, Float> map = new HashMap<>();
    map.put(1.1d, 1.2f);
    map.put(2.2d, 2.3f);
    map.put(3.3d, 3.4f);

    Timestamp timestamp = new Timestamp(123456789l);

    Struct
        struct1 =
        new SimpleStruct(structTypeInfo, Lists.newArrayList(new Date(), new Date(), timestamp));
    Struct
        struct2 =
        new SimpleStruct(structTypeInfo, Lists.newArrayList(null, new Date(), timestamp));
    Struct
        struct3 =
        new SimpleStruct(structTypeInfo, Lists.newArrayList(new Date(), null, timestamp));
    Struct
        struct4 =
        new SimpleStruct(structTypeInfo, Lists.newArrayList(new Date(), new Date(), null));

    Struct
        structC1 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(list, map, struct1));
    Struct
        structC2 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(null, map, struct2));
    Struct
        structC3 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(list, null, struct3));
    Struct
        structC4 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(null, null, struct4));
    record1.set(0, structC1);
    record1.set(1, structC2);
    record1.set(2, structC3);
    record1.set(3, structC4);

    System.out.println(record1.get(0));
    System.out.println(record1.get(1));
    System.out.println(record1.get(2));
    System.out.println(record1.get(3));

    byte[][] bytes = converter.format(record1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    Record record2 = converter.parse(bytes);
    System.out.println(record2.get(0));
    System.out.println(record2.get(1));
    System.out.println(record2.get(2));
    System.out.println(record2.get(3));
    assertEquals(record1.get(0).toString(), record2.get(0).toString());
    assertEquals(record1.get(1).toString(), record2.get(1).toString());
    assertEquals(record1.get(2).toString(), record2.get(2).toString());
    assertEquals(record1.get(3).toString(), record2.get(3).toString());
  }
}
