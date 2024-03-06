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

package com.aliyun.openservices.odps.console.constants;

import com.aliyun.openservices.odps.console.utils.ODPSConsoleUtils;

/**
 * 保存一些console的提示信息
 *
 * @author shuman.gansm
 **/
public class ODPSConsoleConstants {

  // messages
  public final static String BAD_COMMAND = "Bad Command, Type \"help;\"(--help) or \"h;\"(-h) for help. ";
  public final static String UNSUPPORTED_ACCOUNT_PROVIDER = "Unsupported account provider.";
  public final static String NEED_LOGIN_INFO = "Need login info.";
  public final static String COMMAND_END_WITH = "Command must be end with \";\"";
  public final static String INVALID_PARAMETER_E = "Invalid parameter -e";
  public final static String LOAD_CONFIG_ERROR = "Load odps config error";
  public final static String LOAD_VERSION_ERROR = "Load odps version-file error";
  public final static String PROJECT_NOT_BE_SET = "Project not be set! ";
  public final static String EXECUTIONCONTEXT_NOT_BE_SET = "ExecutionContext not be set! ";
  public final static String TABLE_TOO_MANY_COLUMNS = "Too many columns! ";
  public final static String PARTITION_SPC_ERROR = "Partition Spc error! eg : ds='20130406' ";
  public final static String COLUMNS_ERROR = "Columns error! ";
  public final static String COMING_SOON = "Coming soon...";
  public final static String LOCATION_IS_NULL = "Location is null from header";
  public final static String FILE_NOT_EXIST = "File not exist";
  public final static String FILE_UPLOAD_FAIL = "File upload fail";
  public final static String POLICY_SET_FAIL = "Policy Set fail";

  public final static String FILENAME_ENDWITH_PY = "File name must end with py";
  public final static String FILENAME_ENDWITH_JAR = "File name must end with jar";
  public final static String FILENAME_ENDWITH_MORE = "File name(alias name) must end with .jar/.zip/.tgz/.tar.gz/.tar/.whl";

  public final static String MUST_SET_ALIAS = "Must set alias";
  public final static String CANNOT_SET_ALIAS = "Can not set alias";

  public final static String FAILED_MESSAGE = "FAILED: ";

  // interactive command
  public final static String ODPS = "@ODPS";
  public final static String IDENTIFIER = ">";
  public final static String LOGO =
      "          __                         __\n" +
      " ___  ___/ /___   ___ ____ __ _  ___/ /\n" +
      "/ _ \\/ _  // _ \\ (_-</ __//  ' \\/ _  / \n" +
      "\\___/\\_,_// .__//___/\\__//_/_/_/\\_,_/  \n" +
      "         /_/                           ";
  public final static String ALIYUN_ODPS_UTILITIES_VERSION = "Aliyun ODPS Command Line Tool\nVersion " + ODPSConsoleUtils.getMvnVersion() + "\n@Copyright 2020 Alibaba Cloud Computing Co., Ltd. All rights reserved.";
  public final static String ODPS_LOGIN = "ODPS login:";

  // odps_config.ini, properties key
  public static final String PROJECT_NAME = "project_name";
  public static final String SCHEMA_NAME = "schema_name";
  public static final String ACCESS_ID = "access_id";
  public static final String ACCESS_KEY = "access_key";
  public static final String APP_ACCESS_ID = "app_access_id";
  public static final String APP_ACCESS_KEY = "app_access_key";
  public static final String PROXY_HOST = "proxy_host";
  public static final String PROXY_PORT = "proxy_port";
  public static final String END_POINT = "end_point";
  public static final String DATA_SIZE_CONFIRM = "data_size_confirm";
  public static final String UPDATE_URL = "update_url";

  public static final String LOG_VIEW_HOST = "log_view_host";
  public static final String LOG_VIEW_LIFE = "log_view_life";
  public static final String IS_DEBUG = "debug";
  public static final String ACCOUNT_PROVIDER = "account_provider";
  public static final String INSTANCE_PRIORITY = "instance_priority";

  public static final String CUPID_PROXY_END_POINT = "cupid_proxy_end_point";

  // hook class
  public static final String POST_HOOK_CLASS = "post_hook_class";
  public static final String USER_COMMANDS = "user_commands";
  public static final String TUNNEL_ENDPOINT = "tunnel_endpoint";
  public static final String DATAHUB_ENDPOINT = "hub_endpoint";
  public static final String RUNNING_CLUSTER = "running_cluster";

  public static final String HTTPS_CHECK = "https_check";
  public static final String USE_INSTANCE_TUNNEL = "use_instance_tunnel";
  public static final String INSTANCE_TUNNEL_MAX_RECORD = "instance_tunnel_max_record";
  public static final String INSTANCE_TUNNEL_MAX_SIZE = "instance_tunnel_max_size";

  // Session flags
  public static final String INTERACTIVE_AUTO_RERUN = "interactive_auto_rerun";
  public static final String ENABLE_INTERACTIVE_MODE = "enable_interactive_mode";
  public static final String INTERACTIVE_SERVICE_NAME = "interactive_service_name";
  public static final String INTERACTIVE_OUTPUT_COMPATIBLE = "interactive_output_compatible";
  public static final String INTERACTIVE_MAX_ATTACH = "interactive_max_attach";
  public static final String ATTACH_SESSION_TIMEOUT = "attach_session_timeout";

  public static final String FALLBACK_PREFIX = "fallback.";
  public static final String FALLBACK_RESOURCE_NOT_ENOUGH = "fallback.resource";
  public static final String FALLBACK_UNSUPPORTED = "fallback.unsupported";
  public static final String FALLBACK_QUERY_TIMEOUT = "fallback.timeout";
  public static final String FALLBACK_UPGRADING = "fallback.upgrading";
  public static final String FALLBACK_ATTACH_FAILED = "fallback.attach";
  public static final String FALLBACK_UNKNOWN = "fallback.unknown";

  public static final String SESSION_DEFAULT_TASK_NAME = "console_sqlrt_task";
  public static final String ODPS_DEFAULT_SCHEMA = "odps.default.schema";
  public static final String ODPS_NAMESPACE_SCHEMA = "odps.namespace.schema";
  public static final String ODPS_INSTANCE_PRIORITY = "odps.instance.priority";
  public static final String ODPS_READ_LEGACY = "odps.read.legacy";
  public static final String ODPS_SQL_TIMEZONE = "odps.sql.timezone";
  public static final String ODPS_RUNNING_CLUSTER = "odps.running.cluster";
  public static final String CONSOLE_SQL_RESULT_INSTANCETUNNEL = "console.sql.result.instancetunnel";
  public static final String TASK_MAJOR_VERSION = "odps.task.major.version";

  public static final String tablePattern = "";

  // ACL settings
  public static final String OBJECT_CREATOR_HAS_ACCESS_PERMISSION = "OBJECTCREATORHASACCESSPERMISSION";
  public static final String OBJECT_CREATOR_HAS_GRANT_PERMISSION = "OBJECTCREATORHASGRANTPERMISSION";
  public static final String CHECK_PERMISSION_USING_ACL = "CHECKPERMISSIONUSINGACL";
  public static final String CHECK_PERMISSION_USING_POLICY = "CHECKPERMISSIONUSINGPOLICY";
  public static final String PROJECT_PROTECTION = "PROJECTPROTECTION";
  public static final String LABEL_SECURITY = "LABELSECURITY";
  public static final String EXTERNAL_RESOURCE_ACCESS_CONTROL = "EXTERNALRESOURCEACCESSCONTROL";

  // Lite mode
  public static final String LITE_MODE = "lite_mode";

  public static String SQLCOMMANDS = "sqlcommands";

  public static String PRINTCMDS = "printcommand";

  // ODPS SQL reserved words
  public static final String[] ODPS_SQL_RESERVED_WORDS = {"ADD", "AFTER", "ALL", "ALTER",
      "ANALYZE", "AND", "ARCHIVE", "ARRAY", "AS", "ASC", "BEFORE", "BETWEEN", "BIGINT", "BINARY",
      "BLOB", "BOOLEAN", "BOTH", "BUCKET", "BUCKETS", "BY", "CASCADE", "CASE", "CAST", "CFILE",
      "CHANGE", "CLUSTER", "CLUSTERED", "CLUSTERSTATUS", "COLLECTION", "COLUMN", "COLUMNS",
      "COMMENT", "COMPUTE", "CONCATENATE", "CONTINUE", "CREATE", "CROSS", "CURRENT", "CURSOR",
      "DATA", "DATABASE", "DATABASES", "DATE", "DATETIME", "DBPROPERTIES", "DEFERRED", "DELETE",
      "DELIMITED", "DESC", "DESCRIBE", "DIRECTORY", "DISABLE", "DISTINCT", "DISTRIBUTE", "DOUBLE",
      "DROP", "ELSE", "ENABLE", "END", "ESCAPED", "EXCLUSIVE", "EXISTS", "EXPLAIN", "EXPORT",
      "EXTENDED", "EXTERNAL", "FALSE", "FETCH", "FIELDS", "FILEFORMAT", "FIRST", "FLOAT",
      "FOLLOWING", "FORMAT", "FORMATTED", "FROM", "FULL", "FUNCTION", "FUNCTIONS", "GRANT",
      "GROUP", "HAVING", "HOLD_DDLTIME", "IDXPROPERTIES", "IF", "IMPORT", "IN", "INDEX", "INDEXES",
      "INPATH", "INPUTDRIVER", "INPUTFORMAT", "INSERT", "INT", "INTERSECT", "INTO", "IS", "ITEMS",
      "JOIN", "KEYS", "LATERAL", "LEFT", "LIFECYCLE", "LIKE", "LIMIT", "LINES", "LOAD", "LOCAL",
      "LOCATION", "LOCK", "LOCKS", "LONG", "MAP", "MAPJOIN", "MATERIALIZED", "MINUS", "MSCK",
      "NOT", "NO_DROP", "NULL", "OF", "OFFLINE", "ON", "OPTION", "OR", "ORDER", "OUT", "OUTER",
      "OUTPUTDRIVER", "OUTPUTFORMAT", "OVER", "OVERWRITE", "PARTITION", "PARTITIONED",
      "PARTITIONPROPERTIES", "PARTITIONS", "PERCENT", "PLUS", "PRECEDING", "PRESERVE", "PROCEDURE",
      "PURGE", "RANGE", "RCFILE", "READ", "READONLY", "READS", "REBUILD", "RECORDREADER",
      "RECORDWRITER", "REDUCE", "REGEXP", "RENAME", "REPAIR", "REPLACE", "RESTRICT", "REVOKE",
      "RIGHT", "RLIKE", "ROW", "ROWS", "SCHEMA", "SCHEMAS", "SELECT", "SEMI", "SEQUENCEFILE",
      "SERDE", "SERDEPROPERTIES", "SET", "SHARED", "SHOW", "SHOW_DATABASE", "SMALLINT", "SORT",
      "SORTED", "SSL", "STATISTICS", "STORED", "STREAMTABLE", "STRING", "STRUCT", "TABLE",
      "TABLES", "TABLESAMPLE", "TBLPROPERTIES", "TEMPORARY", "TERMINATED", "TEXTFILE", "THEN",
      "TIMESTAMP", "TINYINT", "TO", "TOUCH", "TRANSFORM", "TRIGGER", "TRUE", "UNARCHIVE",
      "UNBOUNDED", "UNDO", "UNION", "UNIONTYPE", "UNIQUEJOIN", "UNLOCK", "UNSIGNED", "UPDATE",
      "USE", "USING", "UTC", "UTC_TMESTAMP", "VIEW", "WHEN", "WHERE", "WHILE", "DIV"};
}
