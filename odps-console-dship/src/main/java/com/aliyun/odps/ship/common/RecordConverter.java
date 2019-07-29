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
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.TimeZone;

import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.Char;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.Varchar;
import com.aliyun.odps.type.TypeInfo;

public class RecordConverter {

  private final byte[] nullBytes;
  private ArrayRecord r = null;
  TableSchema schema;
  String nullTag;
  SimpleDateFormat datetimeFormatter;
  SimpleDateFormat dateFormatter;

  DecimalFormat doubleFormat;
  String charset;
  String defaultCharset;
  boolean isStrictSchema;

  public RecordConverter(TableSchema schema, String nullTag, String dateFormat, String tz,
                         String charset, boolean exponential)
      throws UnsupportedEncodingException {
    this(schema, nullTag, dateFormat, tz, charset, exponential, true);
  }

  public RecordConverter(TableSchema schema, String nullTag, String dateFormat, String tz,
                         String charset, boolean exponential, boolean isStrictSchema)
      throws UnsupportedEncodingException {

    this.schema = schema;
    this.nullTag = nullTag;
    this.isStrictSchema = isStrictSchema;

    if (dateFormat == null) {
      this.datetimeFormatter = new SimpleDateFormat(Constants.DEFAULT_DATETIME_FORMAT_PATTERN);
    } else {
      datetimeFormatter = new SimpleDateFormat(dateFormat);
    }
    datetimeFormatter.setLenient(false);
    if (tz != null) {
      TimeZone t = TimeZone.getTimeZone(tz);
      if (!tz.equalsIgnoreCase("GMT") && t.getID().equals("GMT")) {
        System.err.println(Constants.WARNING_INDICATOR + "possible invalid time zone: " + tz
                           + ", fall back to GMT");
      }
      datetimeFormatter.setTimeZone(t);
    }

    if (exponential) {
      doubleFormat = null;
    } else {
      doubleFormat = new DecimalFormat();

      doubleFormat.setMinimumFractionDigits(0);
      doubleFormat.setMaximumFractionDigits(20);
    }

    setCharset(charset);
    r = new ArrayRecord(schema.getColumns().toArray(new Column[0]));
    nullBytes = nullTag.getBytes(defaultCharset);
    dateFormatter = new SimpleDateFormat(Constants.DEFAULT_DATE_FORMAT_PATTERN);
    dateFormatter.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  public RecordConverter(TableSchema schema, String nullTag, String dateFormat, String tz,
                         String charset)
      throws UnsupportedEncodingException {
    this(schema, nullTag, dateFormat, tz, charset, false);
  }

  public RecordConverter(TableSchema schema, String nullTag, String dateFormat, String tz)
      throws UnsupportedEncodingException {
    this(schema, nullTag, dateFormat, tz, Constants.REMOTE_CHARSET, false);
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
          break;
        }
        default:
          v = r.get(i);
          break;
      }

      line[i] = formatValue(typeInfo, v);
    }
    return line;
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
          return v.toString().replaceAll(",", "").getBytes(defaultCharset);
        }
      }
      case DATETIME: {
        return datetimeFormatter.format(v).getBytes(defaultCharset);
      }
      case DATE: {
        return dateFormatter.format(v).getBytes(defaultCharset);
      }
      case TIMESTAMP: {
        if (((java.sql.Timestamp) v).getNanos() == 0) {
          return datetimeFormatter.format(v).getBytes(defaultCharset);
        } else {
          String res =
              String.format("%s.%s", datetimeFormatter.format(v),
                            ((java.sql.Timestamp) v).getNanos());
          return res.getBytes(defaultCharset);
        }
      }
      case BINARY: {
        return (byte []) v;
      }
      case STRING:
      case CHAR:
      case VARCHAR: {
        if (v instanceof String) {
          v = ((String) v).getBytes(Constants.REMOTE_CHARSET);
        }
        if (Util.isIgnoreCharset(charset)) {
          return (byte [])v;
        } else {
          // data at ODPS side is always utf-8
          return new String((byte[]) v, Constants.REMOTE_CHARSET).getBytes(charset);
        }
      }
      case DECIMAL: {
        return ((BigDecimal)v).toPlainString().getBytes(defaultCharset);
      }
      case ARRAY:
      case MAP:
      case STRUCT: {
        throw new RuntimeException("Not support column type: " + typeInfo);
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
      throw new ParseException(Constants.ERROR_INDICATOR + "column mismatch, expected "
                               + schema.getColumns().size() + " columns, " + line.length
                               + " columns found, please check data or delimiter\n");
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
        throw new ParseException(Constants.ERROR_INDICATOR + "format error - " + ":" + (idx + 1)
                                 + ", " + typeInfo + ":'" + val + "'  " + e.getMessage());
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

      switch (typeInfo.getOdpsType()) {
        case BIGINT: {
          return Long.valueOf(new String(v, defaultCharset));
        }
        case DOUBLE: {
          return Double.valueOf(new String(v, defaultCharset));
        }
        case DATETIME: {
          try {
          return datetimeFormatter.parse(new String(v, defaultCharset));
          } catch (java.text.ParseException e) {
            throw new ParseException(e.getMessage());
          }
        }
        case BOOLEAN: {
          String vStr = new String(v, defaultCharset);
          vStr = vStr.trim().toLowerCase();
          if (vStr.equals("true") || vStr.equals("false")) {
            return vStr.equals("true");
          } else if (vStr.equals("0") || vStr.equals("1")) {
            return vStr.equals("1");
          } else {
            throw new IllegalArgumentException("invalid boolean type, expect: 'true'|'false'|'0'|'1'");
          }
        }
        case STRING: {
          try {
            if (isIgnoreCharset) {
              return v;
            } else {
              return new String(v, charset);
            }
          } catch (IllegalArgumentException e) {
            // for big than 8M
            throw new IllegalArgumentException("string type big than 8M");
          }
        }
        case CHAR: {
          return new Char(new String(v, defaultCharset));
        }
        case VARCHAR: {
          return new Varchar(new String(v, defaultCharset));
        }
        case DECIMAL: {
          return new BigDecimal(new String(v, defaultCharset));
        }
        case INT: {
          return Integer.valueOf(new String(v, defaultCharset));
        }
        case TINYINT: {
          return Byte.valueOf(new String(v, defaultCharset));
        }
        case SMALLINT: {
          return Short.valueOf(new String(v, defaultCharset));
        }
        case FLOAT: {
          return Float.valueOf(new String(v, defaultCharset));
        }
        case BINARY: {
          return new Binary(v);
        }
        case DATE: {
          try {
            java.util.Date dtime = dateFormatter.parse(new String(v, defaultCharset));

            return new java.sql.Date(dtime.getTime());
          } catch (java.text.ParseException e) {
            throw new ParseException(e.getMessage());
          }
        }
        case TIMESTAMP: {
          try {
            String [] res = new String(v, defaultCharset).split("\\.");
            if (res.length > 2) {
              throw  new ParseException("Invalid timestamp value.");
            }
            java.sql.Timestamp timestamp = new java.sql.Timestamp(datetimeFormatter.parse(res[0]).getTime());
            if (res.length == 2 && !res[1].isEmpty()) {
              timestamp.setNanos(Integer.parseInt(res[1]));
            }
            return timestamp;
          } catch (java.text.ParseException e) {
            throw new ParseException(e.getMessage());
          } catch (NumberFormatException ex) {
            throw new ParseException(ex.getMessage());
          }
        }
        case MAP:
        case STRUCT:
        case ARRAY: {
          throw new IllegalArgumentException("Not support column type");
        }
        default:
          throw new IllegalArgumentException("Unknown column type");
      }
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

}
