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
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.cli.ParseException;

import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.data.Record;

public class RecordConverter {

  private final byte[] nullBytes;
  private ArrayRecord r = null;
  TableSchema schema;
  String nullTag;
  SimpleDateFormat dateFormatter;

  DecimalFormat doubleFormat;
  String charset;
  String defaultCharset;

  public RecordConverter(TableSchema schema, String nullTag, String dateFormat, String tz, String charset, boolean exponential)
      throws UnsupportedEncodingException {

    this.schema = schema;
    this.nullTag = nullTag;

    if (dateFormat == null) {
      this.dateFormatter = new SimpleDateFormat(Constants.DEFAULT_DATE_FORMAT_PATTERN);
    } else {
      dateFormatter = new SimpleDateFormat(dateFormat);
    }
    dateFormatter.setLenient(false);
    if (tz != null) {
      TimeZone t = TimeZone.getTimeZone(tz);
      if (!tz.equalsIgnoreCase("GMT") && t.getID().equals("GMT")) {
        System.err.println(Constants.WARNING_INDICATOR + "possible invalid time zone: " + tz
                           + ", fall back to GMT");
      }
      dateFormatter.setTimeZone(t);
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
  }

  public RecordConverter(TableSchema schema, String nullTag, String dateFormat, String tz, String charset)
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
    byte[] colValue = null;
    for (int i = 0; i < cols; i++) {

      OdpsType t = schema.getColumn(i).getType();
      switch (t) {
        case BIGINT: {
          Long v = r.getBigint(i);
          colValue = v == null ? null : v.toString().getBytes(defaultCharset);
          break;
        }
        case DOUBLE: {
          Double v = r.getDouble(i);
          if (v == null) {
            colValue = null;
          } else if (v.equals(Double.POSITIVE_INFINITY)
                     || v.equals(Double.NEGATIVE_INFINITY)) {
            colValue = v.toString().getBytes(defaultCharset);
          } else {
            if (doubleFormat != null) {
              colValue = doubleFormat.format(v).replaceAll(",", "").getBytes(defaultCharset);
            } else {
              colValue = v.toString().replaceAll(",", "").getBytes(defaultCharset);
            }
          }
          break;
        }
        case DATETIME: {
          Date v = r.getDatetime(i);
          if (v == null) {
            colValue = null;
          } else {
            colValue = dateFormatter.format(v).getBytes(defaultCharset);
          }
          break;
        }
        case BOOLEAN: {
          Boolean v = r.getBoolean(i);
          colValue = v == null ? null : v.toString().getBytes(defaultCharset);
          break;
        }
        case STRING: {
          byte[] v = r.getBytes(i);
          if (v == null) {
            colValue = null;
          } else if (Util.isIgnoreCharset(charset)) {
            colValue = v;
          } else {
            // data at ODPS side is always utf-8
            colValue = new String(v, Constants.REMOTE_CHARSET).getBytes(charset);
          }
          break;
        }
        case DECIMAL: {
          BigDecimal v = r.getDecimal(i);
          colValue = v == null ? null : v.toPlainString().getBytes(defaultCharset);
          break;
        }
        default:
          throw new RuntimeException("Unknown column type: " + t);
      }

      if (colValue == null) {
        line[i] = nullBytes;
      } else {
        line[i] = colValue;
      }
    }
    return line;
  }

  /**
   * byte array to tunnel record
   */
  public Record parse(byte[][] line) throws ParseException, UnsupportedEncodingException {

    if (line == null) {
      return null;
    }
    int cols = schema.getColumns().size();

    if (line.length != cols) {
      throw new ParseException(Constants.ERROR_INDICATOR + "column mismatch, expected "
                               + schema.getColumns().size() + " columns, " + line.length
                               + " columns found, please check data or delimiter\n");
    }

    boolean isIgnoreCharset = Util.isIgnoreCharset(charset);

    int idx = 0;
    for (byte[] v : line) {
      OdpsType type = schema.getColumn(idx).getType();
      String eMsg = "";
      try {
        if (Arrays.equals(v, nullBytes)) {
          r.set(idx, null);
          idx++;
          continue;
        }

        switch (type) {
          case BIGINT: {
            String vStr = new String(v, defaultCharset);
            r.setBigint(idx, Long.valueOf(vStr));
            break;
          }
          case DOUBLE: {
            String vStr = new String(v, defaultCharset);
            r.setDouble(idx, Double.valueOf(vStr));
            break;
          }
          case DATETIME: {
            String vStr = new String(v, defaultCharset);
            r.setDatetime(idx, dateFormatter.parse(vStr));
            break;
          }
          case BOOLEAN: {
            String vStr = new String(v, defaultCharset);
            vStr = vStr.trim().toLowerCase();
            if (vStr.equals("true") || vStr.equals("false")) {
              r.setBoolean(idx, vStr.equals("true"));
            } else if (vStr.equals("0") || vStr.equals("1")) {
              r.setBoolean(idx, vStr.equals("1"));
            } else {
              eMsg = "invalid boolean type, expect: 'true'|'false'|'0'|'1'";
              throw new IllegalArgumentException(eMsg);
            }
            break;
          }
          case STRING:
            try {
              if (isIgnoreCharset) {
                r.setString(idx, v);
              } else {
                r.setString(idx, new String(v, charset));
              }
            } catch (IllegalArgumentException e) {
              // for big than 8M
              eMsg = "string type big than 8M";
              throw new IllegalArgumentException(eMsg);
            }
            break;
          case DECIMAL:
            String vStr = new String(v, defaultCharset);
            r.setDecimal(idx, new BigDecimal(vStr));
            break;
          default:
            eMsg = "Unknown column type";
            throw new IllegalArgumentException(eMsg);
        }
      } catch (Exception e) {
        String val;
        String vStr;
        if (isIgnoreCharset) {
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
                                 + ", " + type + ":'" + val + "'  " + eMsg);
      }
      idx++;
    }
    return r;
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
