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

package com.aliyun.openservices.odps.console.tunnel;

import java.util.Properties;

import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.PluginUtil;

public class Config {

  private String charset = "utf8";
  private char colDelimiter = ',';
  private char rowDelimiter = '\n';
  private String nullIndicator = "";
  private String dateFormat = "yyyyMMddHHmmss";
  private boolean badDiscard = false;
  private long maxSize = 100 * 1024 * 1024;

  public long getMaxSize() {
    return maxSize;
  }

  public void setMaxSize(long maxSize) {
    this.maxSize = maxSize;
  }

  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  public char getColDelimiter() {
    return colDelimiter;
  }

  public void setColDelimiter(char colDelimiter) {
    this.colDelimiter = colDelimiter;
  }

  public char getRowDelimiter() {
    return rowDelimiter;
  }

  public void setRowDelimiter(char rowDelimiter) {
    this.rowDelimiter = rowDelimiter;
  }

  public String getNullIndicator() {
    return nullIndicator;
  }

  public void setNullIndicator(String nullIndicator) {
    this.nullIndicator = nullIndicator;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  public void setDateFormat(String dateFormat) {
    this.dateFormat = dateFormat;
  }

  public boolean isBadDiscard() {
    return badDiscard;
  }

  public void setBadDiscard(boolean badDiscard) {
    this.badDiscard = badDiscard;
  }

  public static Config getConfig() throws ODPSConsoleException {

    Config conf = new Config();

    try {
      // load tunnel_endpoint
      Properties properties = PluginUtil.getPluginProperty(Config.class);

      String charset = properties.getProperty("charset");
      if (charset != null) {
        conf.setCharset(charset);
      }

      String nullIndicator = properties.getProperty("null.indicator");
      if (nullIndicator != null) {
        conf.setNullIndicator(nullIndicator);
      }
      String dateFormat = properties.getProperty("date.format");
      if (dateFormat != null) {
        conf.setDateFormat(dateFormat);
      }

      String colDelimiter = properties.getProperty("col.delimiter");
      if (colDelimiter != null) {
        int i = Integer.valueOf(colDelimiter);
        conf.setColDelimiter((char) i);
      }
      String rowDelimiter = properties.getProperty("row.delimiter");
      if (rowDelimiter != null) {
        int i = Integer.valueOf(rowDelimiter);
        conf.setRowDelimiter((char) i);
      }

      String badDiscard = properties.getProperty("bad.discard");
      if (badDiscard != null) {
        conf.setBadDiscard(Boolean.valueOf(badDiscard));
      }

      String max_size = properties.getProperty("max.size");
      if (max_size != null) {
        conf.setMaxSize(Long.valueOf(max_size));
      }

    } catch (Exception e) {
      throw new ODPSConsoleException("pls check plugin.ini.", e);
    }

    return conf;
  }

}
