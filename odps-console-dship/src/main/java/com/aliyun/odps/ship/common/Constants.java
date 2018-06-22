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

public final class Constants {

  public final static int RETRY_LIMIT = 5;
  public final static int RETRY_INTERNAL = 5 * 1000;
  public final static String TUNNEL_INSTANCE_PREFIX = "instance://";

  public final static String TUNNEL_ENDPOINT = "tunnel_endpoint";
  public final static String TABLE_PROJECT = "table-project";
  public final static String CHARSET = "charset";
  public final static String FIELD_DELIMITER = "field-delimiter";
  public final static String RECORD_DELIMITER = "record-delimiter";
  public final static String DISCARD_BAD_RECORDS = "discard-bad-records";
  public final static String DATE_FORMAT_PATTERN = "date-format-pattern";
  public final static String NULL_INDICATOR = "null-indicator";
  public final static String SCAN = "scan";
  public final static String CONFIG_FILE = "config-file";
  public final static String TABLE = "table";
  // for instance download
  public final static String INSTANE_ID = "instance-id";
  public final static String PARTITION_SPEC = "partition-spec";
  public final static String COMMAND = "command";
  public final static String COMMAND_TYPE = "command-type";
  public final static String SESSION_CREATE_TIME = "session-create-time";
  public final static String STATUS = "status";
  public final static String HEADER = "header";
  public static final String AUTO_CREATE_PARTITION = "auto-create-partition";
  public static final String STRICT_SCHEMA = "strict-schema";

  // for resume
  public final static String RESUME_PATH = "resume-path";
  // current block id
  public final static String RESUME_BLOCK_ID = "resume-block-id";
  // current upload id
  public final static String RESUME_UPLOAD_ID = "resume-upload-id";

  // download selected column
  public final static String COLUMNS_NAME = "columns-name";
  public final static String COLUMNS_INDEX = "columns-index";

  // value for option default value
  public final static String REMOTE_CHARSET = "utf8";
  public final static String DEFAULT_FIELD_DELIMITER = ",";
  public final static String DEFAULT_RECORD_DELIMITER = Util.isWindows() ? "\r\n" : "\n";
  public final static String DEFAULT_DISCARD_BAD_RECORDS = "false";
  public final static String DEFAULT_DATETIME_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss"; // for datetime
  public final static String DEFAULT_DATE_FORMAT_PATTERN = "yyyy-MM-dd"; // for date
  public final static String DEFAULT_NULL_INDICATOR = "";
  public final static String DEFAULT_SCAN = "true";
  public final static String DEFAULT_HEADER = "false";
  public static final String DEFAULT_AUTO_CREATE_PARTITION = "false";
  public static final String DEFAULT_STRICT_SCHEMA = "true";

  public final static String DEFAULT_PURGE_NUMBER = "3";
  public static final int DEFAULT_THREADS = 1;
  public static int DEFAULT_IO_BUFFER_SIZE = 50 * 1024 * 1024;
  public static int MAX_RECORD_SIZE = 200 * 1024 * 1024;
  public static long DEFAULT_BLOCK_SIZE = 100;
  public static long DEFAULT_BAD_RECORDS = 1000;

  public static String DEFAULT_SESSION_DIR = Util.getAbsRootDir();

  public static final String IGNORE_CHARSET = "ignore";

  // session id
  public final static String SESSION_ID = "session-id";
  // show command
  public final static String SHOW_COMMAND = "show-command";
  // purge number
  public final static String PURGE_NUMBER = "purge-number";
  // help subcommand
  public final static String HELP_SUBCOMMAND = "help-subcommand";
  public final static String ERROR_INDICATOR = "ERROR: ";
  public final static String WARNING_INDICATOR = "WARNING: ";

  // config settting
  public final static String MAX_BAD_RECORDS = "max-bad-records";
  public final static String COMPRESS = "compress";
  public final static String BLOCK_SIZE = "block-size";
  public final static String TIME_ZONE = "time-zone";
  public final static String LIMIT = "limit";

  public final static String DSHIP_PACKAGE_NAME = "com.aliyun.odps.ship";

  public final static String SESSION_DIR = "session-dir";
  public final static String THREADS = "threads";
  public final static String EXPONENTIAL= "exponential";

}
