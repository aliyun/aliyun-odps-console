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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.UnhandledException;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.SetCommand;
import com.aliyun.openservices.odps.console.constants.ODPSConsoleConstants;
import com.aliyun.openservices.odps.console.utils.Coordinate;
import com.aliyun.openservices.odps.console.utils.FileUtil;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.openservices.odps.console.utils.OdpsConnectionFactory;

public class OptionsBuilder {

  public static void buildUploadOption(String[] args)
      throws ParseException, IOException, ODPSConsoleException {

    DshipContext.INSTANCE.clear();

    CommandLineParser parser = new GnuParser();
    Options opts = getUploadOptions();
    CommandLine line = parser.parse(opts, args);

    initContext();
    // load context from config file
    loadConfig();
    processOptions(line);

    processArgs(line.getArgs(), true);
    checkParameters("upload");

    // set null when user not define RECORD_DELIMITER.
    // later read file to find out the RECORD_DELIMITER(\r\n or \n).
    if (!line.hasOption(Constants.RECORD_DELIMITER)) {
      DshipContext.INSTANCE.put(Constants.RECORD_DELIMITER, null);
    }

    setContextValue(Constants.COMMAND, buildCommand(args));
    setContextValue(Constants.COMMAND_TYPE, "upload");
  }

  public static void buildDownloadOption(String[] args)
      throws ParseException, IOException, ODPSConsoleException, OdpsException {

    DshipContext.INSTANCE.clear();

    CommandLineParser parser = new GnuParser();
    Options opts = getDownloadOptions();
    CommandLine line = parser.parse(opts, args);

    initContext();
    // load context from config file
    loadConfig();
    processOptions(line);
    processArgs(line.getArgs(), false);
    setContextValue(Constants.COMMAND, buildCommand(args));
    setContextValue(Constants.COMMAND_TYPE, "download");

    checkParameters("download");

  }

  private static String buildCommand(String[] args) {

    StringBuilder cl = new StringBuilder();
    for (String a : args) {
      cl.append(a + " ");
    }
    return cl.toString();
  }

  public static void buildResumeOption(String[] args) throws ParseException {

    DshipContext.INSTANCE.clear();

    CommandLineParser parser = new GnuParser();
    Options opts = getResumeOptions();

    CommandLine line = parser.parse(opts, args);
    if (line.hasOption("force")) {
      DshipContext.INSTANCE.put("resume-force", "true");
    }

    String[] remains = line.getArgs();
    if (remains.length == 2) {
      DshipContext.INSTANCE.put(Constants.SESSION_ID, remains[1]);
    } else if (remains.length > 2) {
      throw new IllegalArgumentException("Unknown command\nType 'tunnel help resume' for usage.");
    }
  }

  public static void buildShowOption(String[] args) throws ParseException {

    DshipContext.INSTANCE.clear();

    CommandLineParser parser = new GnuParser();
    Options opts = getShowOptions();
    CommandLine line = parser.parse(opts, args);

    String[] remains = line.getArgs();

    if (remains.length == 3) {
      DshipContext.INSTANCE.put(Constants.SHOW_COMMAND, getShowCmd(remains[1]));
      DshipContext.INSTANCE.put(Constants.SESSION_ID, remains[2]);
    } else if (remains.length == 2) {
      DshipContext.INSTANCE.put(Constants.SHOW_COMMAND, getShowCmd(remains[1]));
    } else {
      throw new IllegalArgumentException("Unknown command\nType 'tunnel help show' for usage.");
    }

    DshipContext.INSTANCE.put("number", line.getOptionValue("number"));
    checkShowCommandParameters();
  }

  private static String getShowCmd(String cmd) {

    if (cmd.equals("h")) {
      return "history";
    } else if (cmd.equals("l")) {
      return "log";
    } else if (cmd.equals("b")) {
      return "bad";
    }
    return cmd;
  }

  public static void buildPurgeOption(String[] args) throws ParseException {

    DshipContext.INSTANCE.clear();

    CommandLineParser parser = new GnuParser();
    Options opts = getPurgeOptions();
    CommandLine line = parser.parse(opts, args);

    String[] remains = line.getArgs();
    if (remains.length == 1) {
      DshipContext.INSTANCE.put(Constants.PURGE_NUMBER, Constants.DEFAULT_PURGE_NUMBER);
    } else if (remains.length == 2) {
      // check parameter

      try {
        Integer.valueOf(remains[1]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Illegal number\nType 'tunnel help purge' for usage.");
      }
      DshipContext.INSTANCE.put(Constants.PURGE_NUMBER, remains[1]);
    } else if (remains.length > 2) {
      throw new IllegalArgumentException("Unknown command\nType 'tunnel help purge' for usage.");
    }

  }

  public static void buildHelpOption(String[] args) throws ParseException {

    DshipContext.INSTANCE.clear();

    CommandLineParser parser = new GnuParser();
    Options opts = getGlobalOptions();

    CommandLine line = parser.parse(opts, args);

    String[] remains = line.getArgs();
    if (remains.length > 2) {
      throw new IllegalArgumentException(
          "Unknown command: too many subcommands.\nType 'tunnel help' for usage.");
    }
    if (remains.length > 1) {
      try {
        CommandType.fromString(remains[1]);
        DshipContext.INSTANCE.put(Constants.HELP_SUBCOMMAND, remains[1]);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Unknown command: " + remains[1] + "\nType 'tunnel help' for usage.");
      }
    }
  }

  private static void checkDelimiters(String type) {
    String fd = DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER);
    if (fd == null || (fd.length() == 0)) {
      String msg = "Field delimiter is null.";
      if ("download".equals(type)) {
        System.err.println("WARNING: " + msg);
      } else {
        throw new IllegalArgumentException(msg);
      }
    }
    String rd = DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER);
    if (rd == null || (rd.length() == 0)) {
      String msg = "Record delimiter is null.";
      if ("download".equals(type)) {
        System.err.println("WARNING: " + msg);
      } else {
        throw new IllegalArgumentException(msg);
      }
    }

    if (fd.contains(rd)) {
      throw new IllegalArgumentException(
          "Field delimiter can not include record delimiter.\nType 'tunnel help " + type
          + "' for usage.");
    }
  }

  public static void checkParameters(String type) {
    checkDelimiters(type);

    String project = DshipContext.INSTANCE.get(Constants.TABLE_PROJECT);
    if (project != null && (project.trim().isEmpty())) {
      throw new IllegalArgumentException(
          "Project is empty.\nType 'tunnel help " + type + "' for usage.");
    }

    /*
      Handle general options
     */
    // charset
    boolean isc = false;
    String c = DshipContext.INSTANCE.get(Constants.CHARSET);
    try {
      isc = Charset.isSupported(c);
    } catch (IllegalCharsetNameException e) {
      //ignore bad charset name
    }
    isc = isc || Constants.IGNORE_CHARSET.equals(c);
    if (c == null || c.isEmpty() || !isc) {
      throw new IllegalArgumentException("Unsupported encoding: '" + c + "'\nType 'tunnel help "
                                         + type + "' for usage.");
    }

    // date format pattern
    try {
      new SimpleDateFormat(DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported date format pattern '"
                                         + DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN)
                                         + "'");
    }

    // TODO: not a parameter from user
    // Make sure session create time initialized
    String sct = DshipContext.INSTANCE.get(Constants.SESSION_CREATE_TIME);
    if (sct == null) {
      throw new IllegalArgumentException(Constants.ERROR_INDICATOR + "create time is null.");
    }

    // compress
    String cp = DshipContext.INSTANCE.get(Constants.COMPRESS);
    if (cp == null) {
      throw new IllegalArgumentException(Constants.ERROR_INDICATOR + "compress info is null.");
    }

    // threads
    int threads;
    try {
      threads = Integer.parseInt(DshipContext.INSTANCE.get(Constants.THREADS));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          Constants.THREADS + " " + DshipContext.INSTANCE.get(Constants.THREADS) + " Invalid.");
    }
    if (threads <= 0) {
      throw new IllegalArgumentException(Constants.THREADS + " argument must > 0.");
    }
    if (threads > 1 && "true".equalsIgnoreCase(DshipContext.INSTANCE.get(Constants.HEADER))) {
      throw new IllegalArgumentException("Do not support write header in multi-threads.");
    }

    String table = DshipContext.INSTANCE.get(Constants.TABLE);

    /*
     Handle download options
     */
    if ("download".equals(type)) {
      // table or instance ID
      if (com.aliyun.odps.utils.StringUtils.isNullOrEmpty(table)
          && com.aliyun.odps.utils.StringUtils
              .isNullOrEmpty(DshipContext.INSTANCE.get(Constants.INSTANE_ID))) {
        throw new IllegalArgumentException("Table or instanceId is null.\nType 'tunnel help " + type
                                           + "' for usage.");
      }

      // limit
      String limit = DshipContext.INSTANCE.get(Constants.LIMIT);
      if (limit != null) {
        try {
          if (Long.valueOf(limit) <= 0) {
            throw new IllegalArgumentException(Constants.LIMIT + " argument must > 0.");
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(Constants.LIMIT + " " + limit + " Invalid.");
        }
      }

      // exponential format
      String exponential = DshipContext.INSTANCE.get(Constants.EXPONENTIAL);
      if (exponential != null && !(exponential.equalsIgnoreCase("true")
                                   || exponential.equalsIgnoreCase("false"))) {
        throw new IllegalArgumentException(
            "Invalid parameter :  exponential format in double expected 'true' or 'false', found '"
            + exponential
            + "'\nType 'tunnel help " + type + "' for usage.");
      }

      // column name & column index
      String columnNames = DshipContext.INSTANCE.get(Constants.COLUMNS_NAME);
      String columnIndexes = DshipContext.INSTANCE.get(Constants.COLUMNS_INDEX);
      if (columnNames != null && columnIndexes != null) {
        throw new IllegalArgumentException(String.format(
            "Invalid parameter, these two params cannot be used together: %s and %s ",
            Constants.COLUMNS_INDEX, Constants.COLUMNS_NAME));
      }
      if (columnIndexes != null) {
        for (String index : columnIndexes.split(",")) {
          if (!StringUtils.isNumeric(index.trim())) {
            throw new IllegalArgumentException(
                "Invalid parameter, columns indexes expected numeric, found " + columnIndexes);
          }
        }
      }
    }

    /*
      Upload options
     */
    if ("upload".equals(type)) {
      // table
      if (com.aliyun.odps.utils.StringUtils.isNullOrEmpty(table)) {
        throw new IllegalArgumentException("Table is null.\nType 'tunnel help " + type
                                           + "' for usage.");
      }

      // scan
      String scan = DshipContext.INSTANCE.get(Constants.SCAN);
      if (scan == null || !(scan.equals("true") || scan.equals("false") || scan.equals("only"))) {
        throw new IllegalArgumentException("-scan, expected:(true|false|only), actual: '" + scan
                                           + "'\nType 'tunnel help " + type + "' for usage.");
      }

      // discard bad records
      String dbr = DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS);
      if (dbr == null || !(dbr.equals("true") || dbr.equals("false"))) {
        throw new IllegalArgumentException(
            "Invalid parameter : discard bad records expected 'true' or 'false', found '" + dbr
            + "'\nType 'tunnel help " + type + "' for usage.");
      }

      // strict schema
      String ss = DshipContext.INSTANCE.get(Constants.STRICT_SCHEMA);
      if (ss == null || !(ss.equals("true") || ss.equals("false"))) {
        throw new IllegalArgumentException(
            "Invalid parameter : strict schema expected 'true' or 'false', found '" + ss
            + "'\nType 'tunnel help " + type + "' for usage.");
      }

      // block size
      String bs = DshipContext.INSTANCE.get(Constants.BLOCK_SIZE);
      if (bs != null) {
        try {
          Long.valueOf(bs);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Illegal number 'block-size', please check config file.");
        }
      }

      // max bad records
      String mbr = DshipContext.INSTANCE.get(Constants.MAX_BAD_RECORDS);
      if (mbr != null) {
        try {
          Long.valueOf(mbr);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Illegal number 'max-bad-records', please check config file.");
        }
      }

      // resume path
      String path = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
      File sFile = new File(path);
      if (!sFile.exists()) {
        throw new IllegalArgumentException("Upload File not found: '" + path
                                           + "'\nType 'tunnel help " + type + "' for usage.");
      }
    }

  }

  private static void checkShowCommandParameters() {

    String cmd = DshipContext.INSTANCE.get(Constants.SHOW_COMMAND);
    String number = DshipContext.INSTANCE.get("number");
    String sid = DshipContext.INSTANCE.get(Constants.SESSION_ID);

    if (!(cmd.equals("log") || cmd.equals("bad") || cmd.equals("history"))) {
      throw new IllegalArgumentException("Unknown command\nType 'tunnel help show' for usage.");
    }
    if ((cmd.equals("log") || cmd.equals("bad")) && (number != null)) {
      throw new IllegalArgumentException("Unknown command\nType 'tunnel help show' for usage.");
    }
    if (cmd.equals("history") && sid != null) {
      throw new IllegalArgumentException("Unknown command\nType 'tunnel help show' for usage.");
    }
    if (number != null) {
      try {
        Integer.valueOf(number);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Illegal number\nType 'tunnel help show' for usage.");
      }
    }
  }

  private static void initContext() {

    // set default value
    setContextValue(Constants.CHARSET, Constants.IGNORE_CHARSET);
    setContextValue(Constants.FIELD_DELIMITER, Constants.DEFAULT_FIELD_DELIMITER);
    setContextValue(Constants.RECORD_DELIMITER, Constants.DEFAULT_RECORD_DELIMITER);
    setContextValue(Constants.DISCARD_BAD_RECORDS, Constants.DEFAULT_DISCARD_BAD_RECORDS);
    setContextValue(Constants.STRICT_SCHEMA, Constants.DEFAULT_STRICT_SCHEMA);
    setContextValue(Constants.DATE_FORMAT_PATTERN, Constants.DEFAULT_DATETIME_FORMAT_PATTERN);
    setContextValue(Constants.NULL_INDICATOR, Constants.DEFAULT_NULL_INDICATOR);
    setContextValue(Constants.SCAN, Constants.DEFAULT_SCAN);
    setContextValue(Constants.HEADER, Constants.DEFAULT_HEADER);
    setContextValue(Constants.RESUME_BLOCK_ID, "1");
    setContextValue(Constants.SESSION_CREATE_TIME,
                    String.valueOf(System.currentTimeMillis()));
    setContextValue(Constants.COMPRESS, "true");
    setContextValue(Constants.THREADS, String.valueOf(Constants.DEFAULT_THREADS));
    setContextValue(Constants.CSV_FORMAT, "false");
    setContextValue(Constants.TIME, Constants.DEFAULT_TIME);
    setContextValue(Constants.OVERWRITE, Constants.DEFAULT_OVERWRITE);
  }

  private static void processOptions(CommandLine line) {
    // set context value from options
    Option[] ops = line.getOptions();
    for (Option op : ops) {
      String v = removeQuote(op.getValue());
      if (Constants.FIELD_DELIMITER.equals(op.getLongOpt())
          || Constants.RECORD_DELIMITER.equals(op.getLongOpt())) {
        setContextValue(op.getLongOpt(), processDelimiter(v));
      } else {
        setContextValue(op.getLongOpt(), v);
      }
    }
  }

  private static final Pattern UNICODE_PATTERN = Pattern.compile("\\\\u([0-9a-fA-F]{4})");

  private static String processDelimiter(String delimiter) {
    if (delimiter != null) {
      delimiter = delimiter.replaceAll("\\\\r", "\r").replaceAll("\\\\n", "\n")
          .replaceAll("\\\\t", "\t");

      Matcher matcher = UNICODE_PATTERN.matcher(delimiter);
      if (matcher.find()) {
        try {
          delimiter = StringEscapeUtils.unescapeJava(delimiter);
        } catch (UnhandledException e) {
          // cannot unescape, use original delimiter
          System.err.println("WARNING: can not recognize delimiter " + delimiter);
        }
      }

      return delimiter;
    }

    return null;
  }

  private static String removeQuote(String in) {
    if (in == null || in.length() < 2) {
      return in;
    }
    if ((in.startsWith("\"") && in.endsWith("\"")) || (in.startsWith("'") && (in.endsWith("'")))) {
      return in.substring(1, in.length() - 1);
    }
    return in;
  }

  /**
   * Parse instance description and set context. For download mode only.
   *
   * @param instanceDesc instance description like: instance://test_project/test_instance
   * @throws IllegalArgumentException
   */
  private static void processInstanceArgs(String instanceDesc) throws IllegalArgumentException {
    String[] instanceDescSplit =
        instanceDesc.substring(Constants.TUNNEL_INSTANCE_PREFIX.length()).split("/");

    if (instanceDescSplit.length == 2) {
      setContextValue(Constants.TABLE_PROJECT, instanceDescSplit[0]);
      setContextValue(Constants.INSTANE_ID, instanceDescSplit[1]);
    } else if (instanceDescSplit.length == 1) {
      setContextValue(Constants.INSTANE_ID, instanceDescSplit[0]);
    } else {
      throw new IllegalArgumentException("Unrecognized command for download instance result\n"
                                         + "Type 'tunnel help download' for usage.");
    }
  }

  /**
   * Process two arguments: source and destination.
   * <p>
   * If the sub command is upload, the destination must be a table.
   * If the sub command is download, the source can either be a table,
   * an instance, or a view
   *
   * @param remains  arguments about upload/download source and destination
   * @param isUpload if the sub command is upload
   * @throws ODPSConsoleException
   */
  private static void processArgs(String[] remains, boolean isUpload)
      throws ODPSConsoleException {
    if (remains.length == 3) {
      String path = isUpload ? remains[1] : remains[2];
      String desc = isUpload ? remains[2] : remains[1];
      String project = null;
      String schema = null;
      String table;
      String partition = null;

      if (!isUpload && desc.startsWith(Constants.TUNNEL_INSTANCE_PREFIX)) {
        processInstanceArgs(desc);
      } else {
        // Description looks like <table name>/<partition spec>
        // or <project name>.<table name>/<partition spec>
        // or <schema name>.<table name>/<partition spec>
        // or <project name>.<schema name>.<table name>/<partition spec>
        String[] descSplit = desc.split("/");
        Coordinate coordinate = Coordinate.getCoordinateABC(descSplit[0]);
        coordinate.interpretByCtx(DshipContext.INSTANCE.getExecutionContext());
        project = coordinate.getProjectName();
        schema = coordinate.getSchemaName();
        table = coordinate.getObjectName();
        if (descSplit.length == 2) {
          partition = new PartitionSpec(descSplit[1]).toString();
        } else if (descSplit.length > 2) {
          throw new IllegalArgumentException("Invalid table identifier: " + desc);
        }

        Odps odps = OdpsConnectionFactory.createOdps(DshipContext.INSTANCE.getExecutionContext());
        odps.setDefaultProject(project);
        odps.setCurrentSchema(schema);
        setContextValue(Constants.TABLE_PROJECT, project);
        setContextValue(Constants.SCHEMA, schema);

        // Handle view
        if (odps.tables().get(project, schema, table).isVirtualView()) {
          // Cannot specify a partition of a view
          if (partition != null) {
            throw new IllegalArgumentException("Invalid view identifier: " + desc);
          }
          // Cannot upload to a view
          if (isUpload) {
            throw new IllegalArgumentException("Invalid operation: upload to a view");
          }
          try {
            // flag=true, select * from a.b.c
            // flag=false, select * from a.c
            //todo schema check
            String tableCoordinate = project + "." + schema + "." + table;
            if (DshipContext.INSTANCE.getExecutionContext().isProjectMode()) {
              tableCoordinate = project + "." + table;
            }
            String query = String.format("SELECT * FROM %s;", tableCoordinate);
            Map<String, String> hints = new HashMap<>();
            hints.put(SetCommand.SQL_DEFAULT_SCHEMA, DshipContext.INSTANCE.get(Constants.SCHEMA));
            hints.put(ODPSConsoleConstants.ODPS_NAMESPACE_SCHEMA,
                      String.valueOf(DshipContext.INSTANCE.getExecutionContext().isOdpsNamespaceSchema()));
            if (SetCommand.setMap.containsKey("odps.sql.allow.namespace.schema")) {
              hints.put("odps.sql.allow.namespace.schema",
                        String.valueOf(DshipContext.INSTANCE.getExecutionContext().isOdpsNamespaceSchema()));
            }
            hints.put(ODPSConsoleConstants.ODPS_NAMESPACE_SCHEMA,
                      String.valueOf(DshipContext.INSTANCE.getExecutionContext().isOdpsNamespaceSchema()));
            Instance instance = SQLTask.run(odps, odps.getDefaultProject(), query, hints, null);
            instance.waitForSuccess();
            processInstanceArgs(Constants.TUNNEL_INSTANCE_PREFIX + instance.getId());
          } catch (OdpsException e) {
            throw new ODPSConsoleException("Read from view failed", e);
          }
        } else {
          setContextValue(Constants.TABLE, table);
          setContextValue(Constants.PARTITION_SPEC, partition);
        }
      }
      setContextValue(Constants.RESUME_PATH, FileUtil.expandUserHomeInPath(path));
    } else {
      throw new IllegalArgumentException("Unrecognized command\nType 'tunnel help "
                                         + (isUpload ? "upload" : "download") + "' for usage.");
    }
  }

  private static void loadConfig() throws IOException, ODPSConsoleException {
    String cf = DshipContext.INSTANCE.getExecutionContext().getConfigFile();

    if (cf == null) {
      return;
    }

    File file = new File(cf);
    if (!file.exists()) {
      return;
    }

    FileInputStream cfIns = new FileInputStream(file);
    Properties properties = new Properties();
    try {
      properties.load(cfIns);
      for (Object key : properties.keySet()) {
        String k = key.toString();
        String v = (String) properties.get(key);
        setContextValue(k.toLowerCase(), v);
      }
    } finally {
      try {
        cfIns.close();
      } catch (IOException e) {
      }
    }
  }

  private static void setContextValue(String key, String value) {

    if (value != null) {
      if (!key.equals(Constants.FIELD_DELIMITER) && !key.equals(Constants.RECORD_DELIMITER)
          && !key.equals(Constants.PARTITION_SPEC)) {
        value = value.trim();
      }
      DshipContext.INSTANCE.put(key, value);
    }
  }

  public static Options getGlobalOptions() {

    Options opts = new Options();

    opts.addOption(OptionBuilder.withLongOpt(Constants.TUNNEL_ENDPOINT).withDescription(
        "tunnel endpoint").hasArg().withArgName("ARG").create("te"));

    opts.addOption(OptionBuilder.withLongOpt(Constants.CHARSET)
                       .withDescription(
                           "specify file charset, default " + Constants.IGNORE_CHARSET + ". set "
                           + Constants.IGNORE_CHARSET + " to download raw data")
                       .hasArg().withArgName("ARG").create("c"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.FIELD_DELIMITER)
                       .withDescription(
                           "specify field delimiter, support unicode, eg \\u0001. default "
                           + Util.toHumanReadableString(Constants.DEFAULT_FIELD_DELIMITER))
                       .hasArg().withArgName("ARG").create("fd"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.RECORD_DELIMITER)
                       .withDescription(
                           "specify record delimiter, support unicode, eg \\u0001. default "
                           + Util.toHumanReadableString(Constants.DEFAULT_RECORD_DELIMITER))
                       .hasArg().withArgName("ARG").create("rd"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.DATE_FORMAT_PATTERN)
                       .withDescription("specify date format pattern, default "
                                        + Constants.DEFAULT_DATETIME_FORMAT_PATTERN)
                       .hasArg().withArgName("ARG").create("dfp"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.NULL_INDICATOR)
                       .withDescription(
                           "specify null indicator string, default "
                           + Util.toHumanReadableString(Constants.DEFAULT_NULL_INDICATOR))
                       .hasArg().withArgName("ARG").create("ni"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.TIME_ZONE)
                       .withDescription("time zone, default local timezone: "
                                        + Calendar.getInstance().getTimeZone().getID())
                       .hasArg().withArgName("ARG").create("tz"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.COMPRESS)
                       .withDescription("compress, default true")
                       .hasArg().withArgName("ARG").create("cp"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.HEADER)
                       .withDescription("if local file should have table header, default "
                                        + Constants.DEFAULT_HEADER).hasArg().withArgName("ARG")
                       .create("h"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.SESSION_DIR)
                       .withDescription("set session dir, default " + Constants.DEFAULT_SESSION_DIR)
                       .hasArg().withArgName("ARG").create("sd"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.THREADS)
                       .withDescription("number of threads, default " + Constants.DEFAULT_THREADS)
                       .hasArg().withArgName("ARG").create("t"));

    opts.addOption(OptionBuilder.withLongOpt(Constants.CSV_FORMAT)
                       .withDescription(
                           "use csv format (true|false), default false. When uploading in csv format, file splitting not supported.")
                       .hasArg().withArgName("ARG").create("cf"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.TIME)
                       .withDescription(
                           "keep track of upload/download elapsed time or not. Default "
                           + Constants.DEFAULT_TIME)
                       .hasArg().withArgName("ARG").create("time"));
    return opts;
  }

  public static Options getUploadOptions() {
    Options opts = getGlobalOptions();
    opts.addOption(OptionBuilder.withLongOpt(Constants.DISCARD_BAD_RECORDS)
                       .withDescription("specify discard bad records action(true|false), default "
                                        + Constants.DEFAULT_DISCARD_BAD_RECORDS)
                       .hasArg().withArgName("ARG").create("dbr"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.SCAN)
                       .withDescription("specify scan file action(true|false|only), default "
                                        + Constants.DEFAULT_SCAN)
                       .hasArg().withArgName("ARG").create("s"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.BLOCK_SIZE)
                       .withDescription("block size in MiB, default "
                                        + Constants.DEFAULT_BLOCK_SIZE)
                       .hasArg().withArgName("ARG").create("bs"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.MAX_BAD_RECORDS)
                       .withDescription("max bad records, default " + Constants.DEFAULT_BAD_RECORDS)
                       .hasArg().withArgName("ARG").create("mbr"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.AUTO_CREATE_PARTITION)
                       .withDescription("auto create target partition if not exists, default "
                                        + Constants.DEFAULT_AUTO_CREATE_PARTITION)
                       .hasArg().withArgName("ARG").create("acp"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.STRICT_SCHEMA)
                       .withDescription(
                           "specify strict schema mode. If false, extra data will be abandoned and insufficient field will be filled with null. Default "
                           + Constants.DEFAULT_STRICT_SCHEMA)
                       .hasArg().withArgName("ARG").create("ss"));
    opts.addOption(
        Option.builder("ow").longOpt(Constants.OVERWRITE).hasArg().argName("true | false")
            .desc("overwrite specified table or partition, default: " + Constants.DEFAULT_OVERWRITE)
            .build());
    return opts;
  }

  public static Options getDownloadOptions() {
    Options opts = getGlobalOptions();
    opts.addOption(OptionBuilder.withLongOpt(Constants.LIMIT)
                       .withDescription("specify the number of records to download")
                       .hasArg().withArgName("ARG").create());
    opts.addOption(OptionBuilder.withLongOpt(Constants.EXPONENTIAL)
                       .withDescription(
                           "When download double values, use exponential express if necessary. Otherwise at most 20 digits will be reserved. Default false")
                       .hasArg().withArgName("ARG").create("e"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.COLUMNS_INDEX)
                       .withDescription(
                           "specify the columns index(starts from 0) to download, use comma to split each index")
                       .hasArg().withArgName("ARG").create("ci"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.COLUMNS_NAME)
                       .withDescription(
                           "specify the columns name to download, use comma to split each name")
                       .hasArg().withArgName("ARG").create("cn"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.WITH_PT)
                       .withDescription(
                           "(true|false)download with partition values in result")
                       .hasArg().withArgName("ARG").create("wp"));
    return opts;
  }

  public static Options getResumeOptions() {
    Options opts = new Options();
    opts.addOption(OptionBuilder.withLongOpt("force").withDescription("force resume").create("f"));
    return opts;
  }

  public static Options getShowOptions() {
    Options opts = new Options();
    opts.addOption(OptionBuilder.withLongOpt("number").withDescription("lines")
                       .hasArg().withArgName("ARG").create("n"));
    return opts;
  }

  public static Options getConfigOptions() {
    return getGlobalOptions();
  }

  public static Options getPurgeOptions() {
    return new Options();
  }

}
