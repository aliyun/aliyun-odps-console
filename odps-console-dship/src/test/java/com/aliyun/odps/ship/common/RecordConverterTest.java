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

import static com.aliyun.odps.OdpsType.ARRAY;
import static com.aliyun.odps.OdpsType.BINARY;
import static com.aliyun.odps.OdpsType.CHAR;
import static com.aliyun.odps.OdpsType.DECIMAL;
import static com.aliyun.odps.OdpsType.FLOAT;
import static com.aliyun.odps.OdpsType.INT;
import static com.aliyun.odps.OdpsType.BIGINT;
import static com.aliyun.odps.OdpsType.DOUBLE;
import static com.aliyun.odps.OdpsType.INTERVAL_DAY_TIME;
import static com.aliyun.odps.OdpsType.INTERVAL_YEAR_MONTH;
import static com.aliyun.odps.OdpsType.JSON;
import static com.aliyun.odps.OdpsType.MAP;
import static com.aliyun.odps.OdpsType.SMALLINT;
import static com.aliyun.odps.OdpsType.STRING;
import static com.aliyun.odps.OdpsType.BOOLEAN;
import static com.aliyun.odps.OdpsType.DATE;
import static com.aliyun.odps.OdpsType.DATETIME;
import static com.aliyun.odps.OdpsType.STRUCT;
import static com.aliyun.odps.OdpsType.TIMESTAMP;
import static com.aliyun.odps.OdpsType.STRING;
import static com.aliyun.odps.OdpsType.TINYINT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
import com.aliyun.odps.type.TypeInfoParser;
import com.google.common.collect.Lists;

/**
 * 测试所有正确的数据类型由string数组<->record的相互转换。
 *
 * 测试目的：<br/> 1) 数据上传需要把string数组转成tunnel的一个record.<br/> 2) 下载需要把rocord转换成string数组。<br/>
 * 将类型分为以下几类进行测试
 *
 * 1 TINYINT, SMALLINT, INT, BIGINT         整形              简单测试 + 上下限
 * FLOAT                                    浮点数             只支持 exp format
 * 1 DOUBLE                                 浮点数             简单测试 + exp format 测试
 * 1 BOOLEAN                                布尔类型           简单测试 + 0/1/true/false parse/format 测试
 * CHAR, VARCHAR, STRING                    字符类型           简单测试 + 编码测试
 * BINARY
 * 1 DECIMAL
 * 1 DATE, DATETIME, TIMESTAMP,               时间类型            新老时间类型测试
 * MAP, ARRAY, STRUCT                       复杂类型            主要是时间相关类型的嵌套测试
 *
 * 测试 NULL 数据
 *
 * INTERVAL_DAY_TIME, INTERVAL_YEAR_MONTH, VOID, JSON, UNKNOWN    不支持
 *
 * 检查步骤
 * 1. check parse value
 * 2. check origin text and format result equal?
 *
 * TODO:
 * 复杂类型测试优化
 */

public class RecordConverterTest {

  TimeZone gmt = TimeZone.getTimeZone("GMT+8");

  private static TableSchema mkTableSchema(Object... o) {
    TableSchema rs = new TableSchema();
    for (int i = 0; i < o.length; i++) {
      if (i % 2 != 0) {
        continue;
      }
      rs.addColumn(new Column((String) o[i], (OdpsType) o[i + 1]));
    }
    return rs;
  }

  private static TableSchema mkTableSchema(OdpsType... types) {
    TableSchema rs = new TableSchema();
    Map<OdpsType, Integer> typeCnt = new HashMap<>();
    for (OdpsType type: types) {
      int cnt = typeCnt.getOrDefault(type, 1);
      rs.addColumn(new Column(type.name() + "_" + cnt, type));
      typeCnt.put(type, cnt + 1);
    }
    return rs;
  }

  private static TableSchema mkTableSchemaComplex(TypeInfo... types) {
    TableSchema rs = new TableSchema();
    Map<TypeInfo, Integer> typeCnt = new HashMap<>();
    for (TypeInfo type: types) {
      int cnt = typeCnt.getOrDefault(type, 1);
      rs.addColumn(new Column(type.getTypeName() + "_" + cnt, type));
      typeCnt.put(type, cnt + 1);
    }
    return rs;
  }

  static class ConverterBuilder {

    TableSchema ts;
    String format = null;
    String charset = Constants.REMOTE_CHARSET;
    String tz = null;
    boolean exp = false;

    ConverterBuilder(TableSchema ts) {
      this.ts = ts;
    }

    ConverterBuilder format(String format) {
      this.format = format;
      return this;
    }

    ConverterBuilder charset(String charset) {
      this.charset = charset;
      return this;
    }

    ConverterBuilder tz(String tz) {
      this.tz = tz;
      return this;
    }

    ConverterBuilder exp(boolean exp) {
      this.exp = exp;
      return this;
    }

    RecordConverter build() throws UnsupportedEncodingException {
      return new RecordConverter(ts, "NULL", format, tz, charset, exp, true);
    }
  }

  private static void equal(TableSchema schema, ArrayRecord record, Object... expected) {
    for (int i = 0; i < schema.getColumns().size(); i++) {
      assertEquals(expected[i], record.get(i));
    }
  }

  private static void originTextEqualFormat(String[] origin, byte[][] format) throws UnsupportedEncodingException {
    for (int i = 0; i < origin.length; i++) {
      assertEquals("converter at index:" + i, origin[i], new String(format[i], "UTF-8"));
    }
  }


  @Test
  public void testParseFormatInteger() throws UnsupportedEncodingException, ParseException {
    TableSchema ts = mkTableSchema(TINYINT, TINYINT, SMALLINT, SMALLINT, INT, INT, BIGINT, BIGINT);
    RecordConverter converter = new ConverterBuilder(ts).build();

    // 测试边界值，注意 MC 的 BIGINT 的最小值是 Long.MIN_VALUE+1
    String[] text = new String[]{"-128", "127",
                                 "-32768", "32767",
                                 "-2147483648", "2147483647",
                                 "-9223372036854775807", "9223372036854775807"};
    ArrayRecord r = (ArrayRecord) converter.parse(toByteArray(text));
    equal(ts, r, Byte.MIN_VALUE, Byte.MAX_VALUE,
          Short.MIN_VALUE, Short.MAX_VALUE,
          Integer.MIN_VALUE, Integer.MAX_VALUE,
          Long.MIN_VALUE+1, Long.MAX_VALUE);
    originTextEqualFormat(text, converter.format(r));

    // MC 不支持 Long.MIN_VALUE，这个会报错
    // 其他范围外不用测试，在 java 的 Type.valueOf 就会报错
    try {
      text = new String[]{"1", "1", "1", "1", "1", "1", "1", "-9223372036854775808"};
      r = (ArrayRecord) converter.parse(toByteArray(text));
    } catch (Exception e) {
      assertTrue("big int min value", e.getMessage()
              .startsWith("ERROR: format error - :8, BIGINT:'-9223372036854775808'"));
    }
  }

  /**
   * 测试布尔类型
   * 上传时文件的取值可以是："true", "false", "1", "0"
   * 下载时，对应的值为"true", "false"
   * 上传数据如果不在这4个值内，抛出脏数据异常，脏数据异常信息符合预期
   */
  @Test
  public void testParseFormatBoolean() throws Exception {
    TableSchema rs = mkTableSchema(BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN);
    RecordConverter converter = new ConverterBuilder(rs).build();

    // 正常情况
    String[] text = new String[]{"true", "false", "1", "0"};
    ArrayRecord r = (ArrayRecord) converter.parse(toByteArray(text));
    equal(rs, r, true, false, true, false);
    originTextEqualFormat(new String[]{"true", "false", "true", "false"}, converter.format(r));

    // 异常情况
    try {
      String[] l2 = new String[]{"true", "false", "1", "a"};
      Record r2 = converter.parse(toByteArray(l2));
      fail("test fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("ERROR: format error - :4, BOOLEAN:'a'") >= 0);
    }
  }

  // 测试普通表示 和 指数表示
  @Test
  public void testParseFormatDouble() throws Exception {
    TableSchema rs = mkTableSchema(DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE);
    RecordConverter expConverter = new ConverterBuilder(rs).exp(true).build();
    RecordConverter nonExpConverter = new ConverterBuilder(rs).exp(false).build();

    String[] text = new String[]{
            "211.234567",
            "Infinity", "-Infinity",
            "12345678.1234567", "1.23456781234567E7", "1.2345E10", // 整数部分 >7 位, Java 默认用 Exponential 显示
            "1.2345E2",                                            // < 7位, 上传时也可以指定为 Exp 表示，下载的 format 不是 Exp
            "1.2345678901234567890123456789"                       // 太长截断
    };
    Record r = expConverter.parse(toByteArray(text));
    equal(rs, (ArrayRecord) r,
          211.234567, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
          12345678.1234567, 1.23456781234567E7, 1.2345E10, 1.2345E2, 1.2345678901234567890123456789);

    String[] expFormat = new String[]{"211.234567", "Infinity", "-Infinity",
                                      "1.23456781234567E7", "1.23456781234567E7", "1.2345E10", "123.45", "1.2345678901234567"};
    String[] nonExpFormat = new String[]{"211.234567", "Infinity", "-Infinity",
                                         "12345678.1234567", "12345678.1234567", "12345000000", "123.45", "1.2345678901234567"};

    originTextEqualFormat(expFormat, expConverter.format(r));
    originTextEqualFormat(nonExpFormat, nonExpConverter.format(r));

    // 异常情况测试
    try {
      String[] l2 = new String[]{"12345678.1234567", "1.23456781234567E7", "1", "a", "b", "1", "1", "1"};
      Record r2 = expConverter.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().indexOf("ERROR: format error - :4, DOUBLE:'a'") >= 0);
    }

  }

  /**
   * 测试日期类型，及pattern出错的情况<br/> 测试目的：<br/> 1） 指定pattern正确，数据正常转换<br/> 2）
   * 指定pattern和数据不一致，抛出脏数据异常，异常符合预期<br/> 3) 上传和下载数据一致<br/>
   */
  @Test
  public void testParseDatetime() throws Exception {

    TableSchema rs = mkTableSchema(DATETIME);
    RecordConverter converter = new ConverterBuilder(rs).format("yyyyMMddHHmmss").build();
    String yyyyMMddHHmmss = "yyyyMMddHHmmss";
    String T2013 = "20131010101010";
    String T1900 = "19000101080000";

    // 1. > GMT 1900 后的时间，新老类型一致，不会有问题
    String[] text = new String[]{T2013};
    SimpleDateFormat oldFormatter = new SimpleDateFormat(yyyyMMddHHmmss);
    ArrayRecord r = (ArrayRecord) converter.parse(toByteArray(text));
    assertEquals("d1 not equal.", oldFormatter.parse(T2013), r.getDatetime(0));
    originTextEqualFormat(text, converter.format(r));

    // 2. <= GMT 1900 的时间，可能有 LMT 的 bug，新老类型不一致
    //    parse / format 走新类型，如果和老 Java Date/SimpleDateFormat 混用，会出现时间偏移
    text = new String[]{T1900};
    oldFormatter = new SimpleDateFormat(yyyyMMddHHmmss);
    DateTimeFormatter newFormatter = DateTimeFormatter.ofPattern(yyyyMMddHHmmss).withZone(ZoneId.systemDefault());
    r = (ArrayRecord) converter.parse(toByteArray(text));
    // BREAKING CHANGE
    assertNotEquals(oldFormatter.parse(T1900), r.getDatetime(0));
    assertEquals(ZonedDateTime.parse(T1900, newFormatter), r.getDatetimeAsZonedDateTime(0));
    originTextEqualFormat(text, converter.format(r));

    // 3. 测试其他时区
    text = new String[]{T2013};
    converter = new ConverterBuilder(rs).format("yyyyMMddHHmmss").tz("GMT+6").build();
    oldFormatter = new SimpleDateFormat("yyyyMMddHHmmss");
    oldFormatter.setTimeZone(TimeZone.getTimeZone("GMT+6"));
    r = (ArrayRecord) converter.parse(toByteArray(text));
    assertEquals(oldFormatter.parse(T2013), r.getDatetime(0));
    originTextEqualFormat(text, converter.format(r));

    // 4. 测试仅日期的 parse
    text = new String[]{"2013-09-25"};
    converter = new ConverterBuilder(rs).format("yyyy-MM-dd").build();
    oldFormatter = new SimpleDateFormat("yyyy-MM-dd");
    r = (ArrayRecord) converter.parse(toByteArray(text));
    assertEquals("d1 not equal.", oldFormatter.parse("2013-09-25"), r.getDatetime(0));

    // 5. 测试错误 text 的 parse
    try {
      String[] l2 = new String[]{T2013};
      Record r2 = converter.parse(toByteArray(l2));
      fail("need fail");
    } catch (Exception e) {
      assertTrue(
          e.getMessage(),
          e.getMessage().indexOf("ERROR: format error - :1, DATETIME:") >= 0);
    }
  }

  @Test
  public void testDate() throws UnsupportedEncodingException, ParseException {
    TableSchema schema = mkTableSchema(DATE, DATE, DATE, DATE);
    // date format 现在是写死的 yyyy-MM-dd, 不在范围的 format 阶段就会出错
    RecordConverter converter = new ConverterBuilder(schema).format("yyyyMMddHHmmss").build();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    String[] text = {"0000-01-01", "1900-01-01", "2022-10-18", "9999-12-31"};

    ArrayRecord record = (ArrayRecord) converter.parse(toByteArray(text));
    assertEquals(LocalDate.of(0, 1, 1), record.getDateAsLocalDate(0));
    assertEquals(LocalDate.of(1900, 1, 1), record.getDateAsLocalDate(1));
    assertEquals(LocalDate.of(2022, 10, 18), record.getDateAsLocalDate(2));
    assertEquals(LocalDate.of(9999, 12, 31), record.getDateAsLocalDate(3));

    originTextEqualFormat(text, converter.format(record));
  }

  @Test
  public void testTimestamp() throws UnsupportedEncodingException, ParseException {
    // 1.测试不指定 format
    TableSchema schema = mkTableSchema(TIMESTAMP, TIMESTAMP, TIMESTAMP);
    RecordConverter converter = new ConverterBuilder(schema).build();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    // 老的 TIMESTAMP parse 行为
    // . >= 2 会报错
    // 毫秒 length > 9 截断，不足补齐

    String[] text = {
            "2022-10-18 10:10:10",
            "2022-10-18 10:10:10.123",
            "2022-10-18 10:10:10.123456789"
    };

    ArrayRecord record = (ArrayRecord) converter.parse(toByteArray(text));
    originTextEqualFormat(text, converter.format(record));

    // 2. 测试指定 format，毫秒必须位数和 format 一样才可以上传
    RecordConverter converter1 = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss.SSSSSS").build();
    String[] text1 = {
            "2022-10-18 10:10:10.123456",
            "2022-10-18 10:10:10.234567",
            "2022-10-18 10:10:10.345678"
    };

    ArrayRecord record1 = (ArrayRecord) converter1.parse(toByteArray(text1));
    originTextEqualFormat(text1, converter1.format(record1));

    String[] text2 = {
            "2022-10-18 10:10:10.123456",
            "2022-10-18 10:10:10.234567",
            "2022-10-18 10:10:10.345678"
    };
    try {
      converter1.parse(toByteArray(text2));
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().startsWith("ERROR: format error - :1, TIMESTAMP"));
    }
  }

   @Test
   public void testDecimal() throws Exception {
     TypeInfo MAX_DECIMAL = TypeInfoFactory.getDecimalTypeInfo(38, 18);
     TableSchema rs = mkTableSchemaComplex(MAX_DECIMAL, MAX_DECIMAL, MAX_DECIMAL);
     RecordConverter converter = new ConverterBuilder(rs).format("yyyyMMddHHmmss").build();

     String[] text = new String[] {"11115111101111511120",
                                   "11115111101111511120.111251113011135138",
                                   "11115111101111511120.11125111301113513811111" // 小数位不会检查，上传到 tunnel 会截断
                                   };
     Record r = converter.parse(toByteArray(text));
     assertEquals("11115111101111511120", r.getDecimal(0).toString());
     assertEquals("11115111101111511120.111251113011135138", r.getDecimal(1).toString());
     assertEquals("11115111101111511120.11125111301113513811111", r.getDecimal(2).toString());
     originTextEqualFormat(text, converter.format(r));

     try {
       Record r2 = converter.parse(toByteArray(new String[]{"11115111101111511120",
                                                            "111151111011115111201",
                                                            "111151111011115111201"
                                                            }));
       // TODO time sdk 只检查了整数位，没有检查小数位
       // 另一个问题，用 tunnel 上传的时候也只检查了整数位，小数位截断处理
       // 但是直接 SQL insert into values ... 小数位是会抛出异常的
       // Record r2 = converter.parse(toByteArray(new String[]{"11115111101111511120", "0.11115111101111511120"}));
       fail("need fail");
     } catch (Exception e) {
       assertTrue(e.getMessage(), e.getMessage().startsWith("ERROR: format error - :2, DECIMAL"));
     }
   }

  /**
   * 测试Null indicator值<br/> 测试目的：<br/> 1）测试所有数据类型的空值上传和下载一致<br/>
   */
  @Test
  public void testParseFormatNull() throws Exception {
    TableSchema schema = mkTableSchema(BIGINT, STRING, DATETIME, BOOLEAN, DOUBLE, DOUBLE, STRING);
    RecordConverter converter = new ConverterBuilder(schema).build();

    String[] text = new String[]{"NULL", "NULL", "NULL", "NULL", "NULL", "NULL", ""};
    Record record = converter.parse(toByteArray(text));
    equal(schema, (ArrayRecord) record, null, null, null, null, null, null, "");
    originTextEqualFormat(text, converter.format(record));

  }

  @Test(expected = ParseException.class)
  public void testColumnMismatch() throws Exception {
    TableSchema rs = mkTableSchema(BIGINT);
    RecordConverter converter = new ConverterBuilder(rs).build();
    String[] text = new String[]{"2", "123"};
    Record r = converter.parse(toByteArray(text));
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

    TableSchema rs = mkTableSchema(STRING);

    // 1. 测试普通中文
    RecordConverter cv = new ConverterBuilder(rs).build();
    String[] text = new String[]{"测试字段"};
    Record r = cv.parse(toByteArray(text));
    assertEquals(text[0], r.getString(0));

    byte[][] l1 = cv.format(r);
    assertEquals("converter at index0:", text[0], new String(l1[0], "UTF-8"));

    //format with charset gbk
    RecordConverter cv2 = new ConverterBuilder(rs).charset("gbk").build();
    r = cv2.parse(toByteArray(text, "gbk"));
    byte[][] l2 = cv2.format(r);
    assertEquals("converter at index0:", text[0], new String(l2[0], "gbk"));

    //format without encoding
    RecordConverter cv3 = new ConverterBuilder(rs).charset(Constants.IGNORE_CHARSET).build();
    byte[][] l3 = cv3.format(r);
    // string is not encoded by gdk charset, use default instead.
    assertEquals("converter at index0:", text[0], new String(l3[0], "UTF-8"));

    //format without encoding
    RecordConverter cv4 = new ConverterBuilder(rs).charset(null).build();
    byte[][] l4 = cv4.format(r);
    // string is not encoded by gdk charset, use default instead.
    assertEquals("converter at index0:", text[0], new String(l4[0], "UTF-8"));
  }

  /**
   * 测试只包含基础类型的Array<br/> 基础类型包括BIGINT, STRING<br/>
   */
  @Test
  public void testArray() throws UnsupportedEncodingException, ParseException {
    TypeInfo typeInfo1 = TypeInfoParser.getTypeInfoFromTypeString("Array<Bigint>");
    TypeInfo typeInfo2 = TypeInfoParser.getTypeInfoFromTypeString("Array<String>");
    TableSchema schema = mkTableSchemaComplex(typeInfo1, typeInfo2);
    RecordConverter converter = new ConverterBuilder(schema).charset(Constants.IGNORE_CHARSET).build();

    Record record1 = new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    List<Long> list1 = Lists.newArrayList(1l, 2l);
    List<String> list2 = Lists.newArrayList(null, "", "null");
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
    TypeInfo typeInfo1 = TypeInfoParser.getTypeInfoFromTypeString("Array<Array<Double>>");
    TypeInfo typeInfo2 = TypeInfoParser.getTypeInfoFromTypeString("Array<Array<String>>");
    TableSchema schema = mkTableSchemaComplex(typeInfo1, typeInfo2);

    RecordConverter converter = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss")
            .charset(Constants.IGNORE_CHARSET).build();

    Record record1 = new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    List<List<Double>> r1 = Lists.newArrayList(
            Lists.newArrayList(1d, 2d),
            Lists.newArrayList(1d, 2d));
    List<List<String>> r2 = Lists.newArrayList(
            Lists.newArrayList(null, "test", "null", ""),
            null,
            new ArrayList<>());
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
    TypeInfo typeInfo = TypeInfoParser.getTypeInfoFromTypeString("Array<Map<String, Datetime>>");
    TableSchema schema = mkTableSchemaComplex(typeInfo);

    RecordConverter converter = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss")
            .charset(Constants.IGNORE_CHARSET).build();

    Record r1 = new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    List<Map<String, Date>> data = Lists.newArrayList(
            Collections.singletonMap("foo", new Date()),
            Collections.singletonMap("bar", new Date()),
            null,
            Collections.EMPTY_MAP,
            Collections.singletonMap(null, new Date()),
            Collections.singletonMap("null", null),
            Collections.singletonMap("", null)
    );
    System.out.println(data);
    System.out.println();
    r1.set(0, data);

    String[] text = {"[\"{\\\"foo\\\":\\\"2022-10-28 14:58:20\\\"}\",\"{\\\"bar\\\":\\\"2022-10-28 14:58:20\\\"}\",\"NULL\",\"{}\",\"{\\\"NULL\\\":\\\"2022-10-28 14:58:20\\\"}\",\"{\\\"null\\\":\\\"NULL\\\"}\",\"{\\\"\\\":\\\"NULL\\\"}\"]"};

    // String[] text = {
    //         "[{\"foo\":\"2022-10-28 12:12:12\"},"
    //         + "{\"bar\":\"2012-10-10 10:10:10\"},"
    //         + "NULL,"
    //         + "{},"
    //         + "{\"NULL\":\"2022-10-10 10:10:10\"},"
    //         + "{\"null\":\"NULL\"},"
    //         + "{\"\":\"NULL\"}]"
    // };
    //
    // Record r2 = converter.parse(toByteArray(text));

    // System.out.println(converter.format(r2));

    System.out.println(r1);
    System.out.println();
    byte[][] bytes = converter.format(r1);
    for (byte[] aByte : bytes) {
      System.out.print(new String(aByte));
      System.out.print(",");
    }
    System.out.println();

    // Record r2 = converter.parse(bytes);
    // List<Map<String, Date>> parsed = (List<Map<String, Date>>) r2.get(0);
    // System.out.println(parsed.toString());
    // Assert.assertEquals(data.toString(), parsed.toString());
  }

  /**
   * 测试嵌套Struct的Array<br/> 基础类型包括 STRING, DATETIME<br/>
   */
  @Test
  public void testArrayWithStruct() throws UnsupportedEncodingException, ParseException {
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoParser.getTypeInfoFromTypeString(
            "Struct<f1:String, f2:Datetime>");
    TypeInfo typeInfo1 = TypeInfoFactory.getArrayTypeInfo(structTypeInfo);
    TableSchema schema = mkTableSchemaComplex(typeInfo1);
    RecordConverter converter = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss.SSS")
            .charset(Constants.IGNORE_CHARSET).build();
    Record record1 = new ArrayRecord(schema.getColumns().toArray(new Column[0]));

    Struct struct1 = new SimpleStruct(structTypeInfo, Lists.newArrayList("", ZonedDateTime.now()));
    Struct struct2 = new SimpleStruct(structTypeInfo, Lists.newArrayList(null, ZonedDateTime.now()));
    Struct struct3 = new SimpleStruct(structTypeInfo, Lists.newArrayList("null", null));

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
    RecordConverter converter = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss")
            .charset(Constants.IGNORE_CHARSET).build();
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
    TypeInfo typeInfo1 = TypeInfoParser.getTypeInfoFromTypeString("Map<String, Date>");
    TypeInfo typeInfo2 = TypeInfoParser.getTypeInfoFromTypeString("Map<Boolean, Map<String, Date>>");
    TableSchema schema = mkTableSchemaComplex(typeInfo1, typeInfo2);
    RecordConverter converter = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss")
            .charset(Constants.IGNORE_CHARSET).build();
    Record record1 = new ArrayRecord(schema.getColumns().toArray(new Column[0]));

    Map<String, LocalDate> map1 = new HashMap<>();
    map1.put(null, LocalDate.now());
    map1.put("test", LocalDate.now());
    map1.put("null", null);
    map1.put(" ", null);

    Map<Boolean, Map<String, LocalDate>> map2 = new HashMap<>();
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
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoParser.getTypeInfoFromTypeString(
            "Struct<f1:String, f2:Datetime>");
    TypeInfo typeInfo1 = TypeInfoParser.getTypeInfoFromTypeString(
            "Map<String, Struct<f1:String, f2:Datetime>>");
    TableSchema schema = mkTableSchemaComplex(typeInfo1);
    RecordConverter converter = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss.SSS")
            .charset(Constants.IGNORE_CHARSET).build();
    Record record1 = new ArrayRecord(schema.getColumns().toArray(new Column[0]));

    Map<String, Struct> map1 = new HashMap<>();
    Struct struct1 = new SimpleStruct(structTypeInfo, Lists.newArrayList("test", ZonedDateTime.now()));
    Struct struct2 = new SimpleStruct(structTypeInfo, Lists.newArrayList(null, ZonedDateTime.now()));
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
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoParser.getTypeInfoFromTypeString(
            "Struct<f1:String, f2:Datetime, f3:Boolean>");
    TypeInfo typeInfo1 = TypeInfoParser.getTypeInfoFromTypeString(
            "Struct<f1:Array<Bigint>, f2:Map<Double, Float>, f3:Struct<f1:String, f2:Datetime, f3:Boolean>>"
    );
    TableSchema schema = mkTableSchemaComplex(typeInfo1, typeInfo1, typeInfo1);

    RecordConverter converter = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss.SSS")
            .charset(Constants.IGNORE_CHARSET).build();
    Record record1 = new ArrayRecord(schema.getColumns().toArray(new Column[0]));

    List list = Lists.newArrayList(1L, 2L, 3L);
    Map<Double, Float> map = new HashMap<>();
    map.put(1.1d, 1.2f);
    map.put(2.2d, 2.3f);
    map.put(3.3d, 3.4f);

    Struct struct1 = new SimpleStruct(structTypeInfo, Lists.newArrayList(" ", ZonedDateTime.now(), true));
    Struct struct2 = new SimpleStruct(structTypeInfo, Lists.newArrayList(null, ZonedDateTime.now(), false));
    Struct struct3 = new SimpleStruct(structTypeInfo, Lists.newArrayList("null", null, false));

    Struct structC1 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(list, map, struct1));
    Struct structC2 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(null, map, struct2));
    Struct structC3 =
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
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoParser.getTypeInfoFromTypeString(
            "Struct<f1:Date, f2:Datetime, f3:Timestamp>" );

    TypeInfo typeInfo1 = TypeInfoParser.getTypeInfoFromTypeString(
            "Struct<f11:Array<Bigint>,"
            + " f22:Map<Double, Float>,"
            + " f33:Struct<f1:Date, f2:Datetime, f3:TImestamp>>"
    );
    TableSchema schema = mkTableSchemaComplex(typeInfo1, typeInfo1, typeInfo1, typeInfo1);
    // new TableSchema();
    // Column c1 = new Column("c1", typeInfo1);
    // Column c2 = new Column("c2", typeInfo1);
    // Column c3 = new Column("c3", typeInfo1);
    // Column c4 = new Column("c4", typeInfo1);
    // schema.addColumn(c1);
    // schema.addColumn(c2);
    // schema.addColumn(c3);
    // schema.addColumn(c4);

    RecordConverter converter = new ConverterBuilder(schema).format("yyyy-MM-dd HH:mm:ss.SSS")
            .charset(Constants.IGNORE_CHARSET).build();
    Record record1 = new ArrayRecord(schema.getColumns().toArray(new Column[0]));

    List list = Lists.newArrayList(1L, 2L, 3L);
    Map<Double, Float> map = new HashMap<>();
    map.put(1.1d, 1.2f);
    map.put(2.2d, 2.3f);
    map.put(3.3d, 3.4f);

    Instant timestamp = Instant.now();

    LocalDate testDate = LocalDate.now();
    ZonedDateTime testDatetime = ZonedDateTime.now();
    Struct struct1 =
        new SimpleStruct(structTypeInfo, Lists.newArrayList(testDate, testDatetime, timestamp));
    Struct struct2 =
        new SimpleStruct(structTypeInfo, Lists.newArrayList(null, testDatetime, timestamp));
    Struct struct3 =
        new SimpleStruct(structTypeInfo, Lists.newArrayList(testDate, null, timestamp));
    Struct struct4 =
        new SimpleStruct(structTypeInfo, Lists.newArrayList(testDate, testDatetime, null));

    Struct structC1 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(list, map, struct1));
    Struct structC2 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(null, map, struct2));
    Struct structC3 =
        new SimpleStruct((StructTypeInfo) typeInfo1, Lists.newArrayList(list, null, struct3));
    Struct structC4 =
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
