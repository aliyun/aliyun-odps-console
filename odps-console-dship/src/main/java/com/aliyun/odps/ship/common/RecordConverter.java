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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class RecordConverter {

  private final byte[] nullBytes;
  private final ArrayRecord r;
  TableSchema schema;
  String nullTag;
  DateTimeFormatter zonedDatetimeFormatter;
  DateTimeFormatter dateFormatter;
  DateTimeFormatter timestampFormatter;

  DecimalFormat doubleFormat;
  String charset;
  ZoneId zoneId;
  String defaultCharset;
  boolean isStrictSchema;

  private final Gson gson = new Gson();
  private final JsonParser jsonParser = new JsonParser();

  public RecordConverter(TableSchema schema, String nullTag,
                         String datetimeFormat, String tz,
                         String charset, boolean exponential, boolean isStrictSchema)
      throws UnsupportedEncodingException {

    this.schema = schema;
    this.nullTag = nullTag;
    this.isStrictSchema = isStrictSchema;

    // 1. setup Date/Datetime/Timestamp formatter
    if (datetimeFormat == null) {
      this.zonedDatetimeFormatter = DateTimeFormatter.ofPattern(Constants.DEFAULT_DATETIME_FORMAT_PATTERN);
      this.timestampFormatter = new DateTimeFormatterBuilder().appendPattern(Constants.DEFAULT_DATETIME_FORMAT_PATTERN)
              .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).toFormatter();
    } else {
      this.zonedDatetimeFormatter = DateTimeFormatter.ofPattern(datetimeFormat);
      this.timestampFormatter = DateTimeFormatter.ofPattern(datetimeFormat);
    }

    //TODO use project timezone default
    this.zoneId = ZoneId.systemDefault();
    if (tz != null) {
      this.zoneId = TimeZone.getTimeZone(tz).toZoneId();
      if (!tz.equalsIgnoreCase("GMT") && this.zoneId.getId().equals("GMT")) {
        System.err.println(Constants.WARNING_INDICATOR + "possible invalid time zone: " + tz + ", fall back to GMT");
      }
    }

    zonedDatetimeFormatter = zonedDatetimeFormatter.withZone(this.zoneId);
    timestampFormatter = timestampFormatter.withZone(this.zoneId);
    dateFormatter = DateTimeFormatter.ofPattern(Constants.DEFAULT_DATE_FORMAT_PATTERN)
            .withResolverStyle(ResolverStyle.LENIENT); // support parse 0000-01-01

    if (exponential) {
      doubleFormat = null;
    } else {
      doubleFormat = new DecimalFormat();
      doubleFormat.setMinimumFractionDigits(0);
      // max double fraction dights is 16
      doubleFormat.setMaximumFractionDigits(20);
    }

    setCharset(charset);
    r = new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    nullBytes = nullTag.getBytes(defaultCharset);
  }

  /**
   * tunnel record to byte[] array
   */
  public byte[][] format(Record r) throws UnsupportedEncodingException {

    int cols = schema.getColumns().size();
    byte[][] line = new byte[cols][];
    Object v = null;
    for (int i = 0; i < cols; i++) {
      TypeInfo typeInfo = schema.getColumn(i).getTypeInfo();
      switch (typeInfo.getOdpsType()) {
        case STRING:
        case CHAR:
        case VARCHAR:
        case BINARY: {
          v = r.getBytes(i);
          line[i] = formatValue(typeInfo, v);
          break;
        }
        case ARRAY: {
          v = r.get(i);
          line[i] = formatValueArray((ArrayTypeInfo) typeInfo, (List) v).getBytes(defaultCharset);
          break;
        }
        case MAP: {
          v = r.get(i);
          line[i] = formatValueMap((MapTypeInfo) typeInfo, (Map) v).getBytes(defaultCharset);
          break;
        }
        case STRUCT: {
          v = r.get(i);
          line[i] =
              formatValueStruct((StructTypeInfo) typeInfo, (Struct) v).getBytes(defaultCharset);
          break;
        }
        default: {
          v = r.get(i);
          line[i] = formatValue(typeInfo, v);
          break;
        }
      }

    }
    return line;
  }

  private String formatValueArray(ArrayTypeInfo typeInfo, List<Object> list)
      throws UnsupportedEncodingException {
    if (list == null) {
      return nullTag;
    }

    TypeInfo elementTypeInfo = typeInfo.getElementTypeInfo();
    List<Object> ret = new ArrayList<>(list.size());
    for (Object o : list) {
      switch (elementTypeInfo.getOdpsType()) {
        case ARRAY:
          ret.add(formatValueArray((ArrayTypeInfo) elementTypeInfo, (List) o));
          break;
        case MAP:
          ret.add(formatValueMap((MapTypeInfo) elementTypeInfo, (Map) o));
          break;
        case STRUCT:
          ret.add(formatValueStruct((StructTypeInfo) elementTypeInfo, (Struct) o));
          break;
        default:
          ret.add(new String(formatValue(elementTypeInfo, o), defaultCharset));
          break;
      }
    }

    return gson.toJson(ret);
  }


  private String formatValueMap(MapTypeInfo typeInfo, Map<Object, Object> map)
      throws UnsupportedEncodingException {
    if (map == null) {
      return nullTag;
    }

    Map<Object, Object> ret = new LinkedHashMap<>(map.size());
    TypeInfo keyTypeInfo = typeInfo.getKeyTypeInfo();
    TypeInfo valueTypeInfo = typeInfo.getValueTypeInfo();

    for (Object key : map.keySet()) {
      Object value = map.get(key);

      switch (keyTypeInfo.getOdpsType()) {
        case ARRAY:
          key = formatValueArray((ArrayTypeInfo) keyTypeInfo, (List) key);
          break;
        case MAP:
          key = formatValueMap((MapTypeInfo) keyTypeInfo, (Map) key);
          break;
        case STRUCT:
          key = formatValueStruct((StructTypeInfo) keyTypeInfo, (Struct) key);
          break;
        default:
          key = new String(formatValue(keyTypeInfo, key), defaultCharset);
          break;
      }

      switch (valueTypeInfo.getOdpsType()) {
        case ARRAY:
          value = formatValueArray((ArrayTypeInfo) valueTypeInfo, (List) value);
          break;
        case MAP:
          value = formatValueMap((MapTypeInfo) valueTypeInfo, (Map) value);
          break;
        case STRUCT:
          value = formatValueStruct((StructTypeInfo) valueTypeInfo, (Struct) value);
          break;
        default:
          value = new String(formatValue(valueTypeInfo, value), defaultCharset);
          break;
      }

      ret.put(key, value);
    }

    return gson.toJson(ret);
  }

  private String formatValueStruct(StructTypeInfo typeInfo, Struct struct)
      throws UnsupportedEncodingException {
    if (struct == null) {
      return nullTag;
    }

    List<Object> values = new ArrayList<>(struct.getFieldCount());

    List<TypeInfo> fieldTypeInfos = typeInfo.getFieldTypeInfos();

    for (int i = 0; i < typeInfo.getFieldCount(); ++i) {
      TypeInfo fieldTypeInfo = fieldTypeInfos.get(i);
      Object o = struct.getFieldValue(i);

      switch (fieldTypeInfo.getOdpsType()) {
        case ARRAY:
          o = formatValueArray((ArrayTypeInfo) fieldTypeInfo, (List) o);
          break;
        case MAP:
          o = formatValueMap((MapTypeInfo) fieldTypeInfo, (Map) o);
          break;
        case STRUCT:
          o = formatValueStruct((StructTypeInfo) fieldTypeInfo, (Struct) o);
          break;
        default:
          o = new String(formatValue(fieldTypeInfo, o), defaultCharset);
          break;
      }
      values.add(o);
    }

    Struct newStruct = new SimpleStruct(typeInfo, values);
    return structToJson(newStruct);
  }

  /**
   * 自定义的Struct转Json的方法<br/> 目的: 减少打印无关信息, 将fieldName展示出来<br/>
   */
  private String structToJson(Struct struct) {
    List<Object> values = struct.getFieldValues();
    Map<String, Object> info = new LinkedHashMap<>();

    for (int i = 0; i < values.size(); i++) {
      String fieldName = struct.getFieldName(i);
      Object val = values.get(i);
      if (val == null) {
        info.put(fieldName, nullTag);
        continue;
      }
      info.put(fieldName, val);
    }

    return gson.toJson(info);
  }

  private byte[] formatValue(TypeInfo typeInfo, Object v) throws UnsupportedEncodingException {
    if (v == null) {
      return nullBytes;
    }

    switch (typeInfo.getOdpsType()) {
      case BIGINT:
      case INT:
      case SMALLINT:
      case TINYINT:
      case BOOLEAN: {
        return v.toString().getBytes(defaultCharset);
      }
      case DOUBLE: {
        if (v.equals(Double.POSITIVE_INFINITY)
            || v.equals(Double.NEGATIVE_INFINITY)) {
          return v.toString().getBytes(defaultCharset);
        } else {
          if (doubleFormat != null) {
            return doubleFormat.format(v).replaceAll(",", "").getBytes(defaultCharset);
          } else {
            return v.toString().replaceAll(",", "").getBytes(defaultCharset);
          }
        }
      }
      case FLOAT: {
        if (v.equals(Float.POSITIVE_INFINITY)
            || v.equals(Float.NEGATIVE_INFINITY)) {
          return v.toString().getBytes(defaultCharset);
        } else {
          // FLOAT 不支持 double format
          return v.toString().replaceAll(",", "").getBytes(defaultCharset);
        }
      }
      case DATETIME: {
        return zonedDatetimeFormatter.format((ZonedDateTime)v).getBytes(defaultCharset);
      }
      case DATE: {
        return ((LocalDate) v).toString().getBytes(defaultCharset);
      }
      case TIMESTAMP: {
        return timestampFormatter.format((Instant)v).getBytes(defaultCharset);
      }
      case BINARY: {
        return (byte[]) v;
      }
      case STRING:
      case CHAR:
      case VARCHAR: {
        if (v instanceof String) {
          v = ((String) v).getBytes(Constants.REMOTE_CHARSET);
        }
        if (Util.isIgnoreCharset(charset)) {
          return (byte[]) v;
        } else {
          // data at ODPS side is always utf-8
          return new String((byte[]) v, Constants.REMOTE_CHARSET).getBytes(charset);
        }
      }
      case DECIMAL: {
        return ((BigDecimal) v).toPlainString().getBytes(defaultCharset);
      }
      case ARRAY:
      case MAP:
      case STRUCT: {
        throw new RuntimeException("Not supported column type: " + typeInfo);
      }
      default:
        throw new RuntimeException("Unknown column type: " + typeInfo);
    }
  }

  /**
   * byte array to tunnel record
   */
  public Record parse(byte[][] line) throws ParseException, UnsupportedEncodingException {
    if (line == null) {
      return null;
    }
    int cols = schema.getColumns().size();

    if (isStrictSchema && line.length != cols) {
      throw new ParseException(Constants.ERROR_INDICATOR + "column mismatch, expected " +
                               schema.getColumns().size() + " columns, " + line.length +
                               " columns found, please check data or delimiter\n");
    }

    int idx = 0;
    for (byte[] v : line) {
      if (idx >= cols && !isStrictSchema) {
        break;
      }

      TypeInfo typeInfo = schema.getColumn(idx).getTypeInfo();

      try {
        r.set(idx, parseValue(typeInfo, v));
      } catch (Exception e) {
        String val;
        String vStr;
        if (Util.isIgnoreCharset(charset)) {
          vStr = new String(v, Constants.REMOTE_CHARSET);
        } else {
          vStr = new String(v, charset);
        }
        if (vStr.length() > 20) {
          val = vStr.substring(0, 17) + "...";
        } else {
          val = vStr;
        }
        throw new ParseException(Constants.ERROR_INDICATOR + "format error - " + ":" + (idx + 1) +
                                 ", " + typeInfo + ":'" + val + "'  " + ExceptionUtils
                                     .getFullStackTrace(e));
      }

      idx++;
    }
    return r;
  }

  private Object parseValue(TypeInfo typeInfo, byte[] v)
      throws UnsupportedEncodingException, ParseException {
    if (Arrays.equals(v, nullBytes)) {
      return null;
    }

    boolean isIgnoreCharset = Util.isIgnoreCharset(charset);
    OdpsType type = typeInfo.getOdpsType();
    String value = getTrimmedString(v, defaultCharset);

    switch (type) {
      case BIGINT: {
        return Long.valueOf(value);
      }
      case DOUBLE: {
        return Double.valueOf(value);
      }
      case DATETIME: {
        try {
          // support Date format yyyy-MM-dd for compatible
          TemporalAccessor accessor = zonedDatetimeFormatter.parseBest(value, ZonedDateTime::from, LocalDate::from);
          if (accessor instanceof LocalDate) {
            accessor = ((LocalDate) accessor).atStartOfDay(zoneId);
          }
          return accessor;
        } catch (RuntimeException e) {
          throw new ParseException(e.getMessage());
        }
      }
      case BOOLEAN: {
        String vStr = value.toLowerCase();
        if ("true".equals(vStr) || "false".equals(vStr)) {
          return "true".equals(vStr);
        } else if ("0".equals(vStr) || "1".equals(vStr)) {
          return "1".equals(vStr);
        } else {
          throw new IllegalArgumentException(
              "Invalid boolean value, expect: 'true'|'false'|'0'|'1'");
        }
      }
      case STRING: {
        try {
          if (isIgnoreCharset) {
            return v;
          } else {
            return getTrimmedString(v, charset);
          }
        } catch (IllegalArgumentException e) {
          // for big than 8M
          throw new IllegalArgumentException("String value bigger than 8M");
        }
      }
      case CHAR: {
        return new Char(value);
      }
      case VARCHAR: {
        return new Varchar(value);
      }
      case DECIMAL: {
        return new BigDecimal(value);
      }
      case INT: {
        return Integer.valueOf(value);
      }
      case TINYINT: {
        return Byte.valueOf(value);
      }
      case SMALLINT: {
        return Short.valueOf(value);
      }
      case FLOAT: {
        return Float.valueOf(value);
      }
      case BINARY: {
        return new Binary(v);
      }
      case DATE: {
        return LocalDate.parse(value, dateFormatter);
      }
      case TIMESTAMP: {
        try {
          // 不兼容 nano > 9 位的情况
          // 1. 用户不指定 format，用标准的 pattern + 0-9 nano
          // 2. 指定 format, 按用户 format 走
          ZonedDateTime dateTime  = ZonedDateTime.parse(value, timestampFormatter);
          return dateTime.toInstant();
        } catch (RuntimeException e) {
          throw new ParseException(e.getMessage());
        }
      }
      case MAP: {
        JsonElement map = jsonParser.parse(value);
        // 当数据为null(字符串)时,会被jsonParser解析为JsonNull类型:
        // 1. 当nullTag为NULL/null/"",会在函数入口处返回null。
        // 2. 当nullTag不为NULL/null/"",需要在此处提前中止,返回null。
        // 以下同理
        if (map.isJsonNull()) {
          return null;
        }
        return transformMap(map.getAsJsonObject(), (MapTypeInfo) typeInfo);
      }
      case STRUCT: {
        JsonElement struct = jsonParser.parse(value);
        if (struct.isJsonNull()) {
          return null;
        }
        return transformStruct(struct.getAsJsonObject(), (StructTypeInfo) typeInfo);
      }
      case ARRAY: {
        JsonElement array = jsonParser.parse(value);
        if (array.isJsonNull()) {
          return null;
        }
        return transformArray(array.getAsJsonArray(), (ArrayTypeInfo) typeInfo);
      }
      default:
        throw new IllegalArgumentException("Unknown column type: " + typeInfo);
    }
  }

  private List<Object> transformArray(JsonArray arrayObj, ArrayTypeInfo typeInfo)
      throws UnsupportedEncodingException, ParseException {

    List<Object> newList = new ArrayList<>();
    TypeInfo elementTypeInfo = typeInfo.getElementTypeInfo();

    for (JsonElement element : arrayObj) {
      switch (elementTypeInfo.getOdpsType()) {
        case ARRAY:
          JsonElement array = jsonParser.parse(element.getAsString());
          // 当数据为null(字符串)时:
          // 1. nullTag为null/NULL/""时, 会被jsonParser解析为JsonNull, 此时走isJsonNull()的逻辑
          // 2. 当nullTag不为null/NULL/""时, 会被jsonParser解析为JsonPrimitive, 此时需要判断解析后的字符串与nullTag是否相等
          // 以下同理
          if (array.isJsonNull() || (array.isJsonPrimitive() && array.getAsString()
              .equals(nullTag))) {
            newList.add(null);
          } else {
            newList.add(transformArray(array.getAsJsonArray(), (ArrayTypeInfo) elementTypeInfo));
          }
          break;
        case MAP:
          JsonElement map = jsonParser.parse(element.getAsString());
          if (map.isJsonNull() || (map.isJsonPrimitive() && map.getAsString().equals(nullTag))) {
            newList.add(null);
          } else {
            newList.add(transformMap(map.getAsJsonObject(), (MapTypeInfo) elementTypeInfo));
          }
          break;
        case STRUCT:
          JsonElement struct = jsonParser.parse(element.getAsString());
          if (struct.isJsonNull() || (struct.isJsonPrimitive() && struct.getAsString()
              .equals(nullTag))) {
            newList.add(null);
          } else {
            newList
                .add(transformStruct(struct.getAsJsonObject(), (StructTypeInfo) elementTypeInfo));
          }
          break;
        default:
          newList.add(parseValue(elementTypeInfo, element.getAsString().getBytes(defaultCharset)));
          break;
      }
    }

    return newList;
  }

  private Map<Object, Object> transformMap(JsonObject mapObj, MapTypeInfo typeInfo)
      throws UnsupportedEncodingException, ParseException {

    TypeInfo keyTypeInfo = typeInfo.getKeyTypeInfo();
    TypeInfo valTypeInfo = typeInfo.getValueTypeInfo();
    Map<Object, Object> newMap = new LinkedHashMap<>();

    for (Map.Entry<String, JsonElement> entry : mapObj.entrySet()) {
      String keyStr = entry.getKey();
      JsonElement value = entry.getValue();
      Object newKey;
      Object newValue;

      switch (keyTypeInfo.getOdpsType()) {
        case MAP:
          JsonElement map = jsonParser.parse(keyStr);
          if (map.isJsonNull() || (map.isJsonPrimitive() && map.getAsString().equals(nullTag))) {
            newKey = null;
          } else {
            newKey = transformMap(map.getAsJsonObject(), (MapTypeInfo) keyTypeInfo);
          }
          break;
        case ARRAY:
          JsonElement array = jsonParser.parse(keyStr);
          if (array.isJsonNull() || (array.isJsonPrimitive() && array.getAsString()
              .equals(nullTag))) {
            newKey = null;
          } else {
            newKey = transformArray(array.getAsJsonArray(), (ArrayTypeInfo) keyTypeInfo);
          }
          break;
        case STRUCT:
          JsonElement struct = jsonParser.parse(keyStr);
          if (struct.isJsonNull() || (struct.isJsonPrimitive() && struct.getAsString()
              .equals(nullTag))) {
            newKey = null;
          } else {
            newKey = transformStruct(struct.getAsJsonObject(), (StructTypeInfo) keyTypeInfo);
          }
          break;
        default:
          newKey = parseValue(keyTypeInfo, keyStr.getBytes(defaultCharset));
      }

      switch (valTypeInfo.getOdpsType()) {
        case MAP:
          JsonElement map = jsonParser.parse(value.getAsString());
          if (map.isJsonNull() || (map.isJsonPrimitive() && map.getAsString().equals(nullTag))) {
            newValue = null;
          } else {
            newValue = transformMap(map.getAsJsonObject(), (MapTypeInfo) valTypeInfo);
          }
          break;
        case ARRAY:
          JsonElement array = jsonParser.parse(value.getAsString());
          if (array.isJsonNull() || (array.isJsonPrimitive() && array.getAsString()
              .equals(nullTag))) {
            newValue = null;
          } else {
            newValue = transformArray(array.getAsJsonArray(), (ArrayTypeInfo) valTypeInfo);
          }
          break;
        case STRUCT:
          JsonElement struct = jsonParser.parse(value.getAsString());
          if (struct.isJsonNull() || (struct.isJsonPrimitive() && struct.getAsString()
              .equals(nullTag))) {
            newValue = null;
          } else {
            newValue = transformStruct(struct.getAsJsonObject(), (StructTypeInfo) valTypeInfo);
          }
          break;
        default:
          newValue = parseValue(valTypeInfo, value.getAsString().getBytes(defaultCharset));
      }

      newMap.put(newKey, newValue);
    }

    return newMap;
  }

  private Struct transformStruct(JsonObject structObj, StructTypeInfo typeInfo)
      throws UnsupportedEncodingException, ParseException {

    List<Object> values = new ArrayList<>();
    List<TypeInfo> fieldTypeInfos = typeInfo.getFieldTypeInfos();

    int index = 0;
    for (Map.Entry<String, JsonElement> entry : structObj.entrySet()) {
      TypeInfo fieldTypeInfo = fieldTypeInfos.get(index++);
      JsonElement element = entry.getValue();

      switch (fieldTypeInfo.getOdpsType()) {
        case ARRAY:
          JsonElement array = jsonParser.parse(element.getAsString());
          if (array.isJsonNull() || (array.isJsonPrimitive() && array.getAsString()
              .equals(nullTag))) {
            values.add(null);
          } else {
            values.add(transformArray(array.getAsJsonArray(), (ArrayTypeInfo) fieldTypeInfo));
          }
          break;
        case MAP:
          JsonElement map = jsonParser.parse(element.getAsString());
          if (map.isJsonNull() || (map.isJsonPrimitive() && map.getAsString().equals(nullTag))) {
            values.add(null);
          } else {
            values.add(transformMap(map.getAsJsonObject(), (MapTypeInfo) fieldTypeInfo));
          }
          break;
        case STRUCT:
          JsonElement struct = jsonParser.parse(element.getAsString());
          if (struct.isJsonPrimitive() && struct.getAsString().equals(nullTag)) {
            values.add(null);
          } else {
            values.add(transformStruct(struct.getAsJsonObject(), (StructTypeInfo) fieldTypeInfo));
          }
          break;
        default:
          values.add(parseValue(fieldTypeInfo, element.getAsString().getBytes(defaultCharset)));
          break;
      }
    }

    return new SimpleStruct(typeInfo, values);
  }

  private void setCharset(String charset) {
    if (Util.isIgnoreCharset(charset)) {
      this.charset = null;
      this.defaultCharset = Constants.REMOTE_CHARSET;
    } else {
      this.charset = charset;
      this.defaultCharset = charset;
    }
  }

  private String getTrimmedString(byte[] v, String charset) throws UnsupportedEncodingException {
    return (new String(v, charset)).trim();
  }

}
