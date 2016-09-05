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
import org.apache.commons.lang.UnhandledException;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.utils.FileUtil;

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
      throws ParseException, IOException, ODPSConsoleException {

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

  public static void checkParameters(String type) {

    String table = DshipContext.INSTANCE.get(Constants.TABLE);

    if (table == null || "".equals(table.trim())) {
      throw new IllegalArgumentException("Table is null.\nType 'tunnel help " + type
                                         + "' for usage.");
    }

    String fd = DshipContext.INSTANCE.get(Constants.FIELD_DELIMITER);
    if (fd == null || fd.length() == 0) {
      throw new IllegalArgumentException("Field delimiter is null.");
    }
    String rd = DshipContext.INSTANCE.get(Constants.RECORD_DELIMITER);
    if (rd == null || rd.length() == 0) {
      throw new IllegalArgumentException("Record delimiter is null.");
    }
    if (fd.contains(rd)) {
      throw new IllegalArgumentException(
          "Field delimiter can not include record delimiter.\nType 'tunnel help " + type
          + "' for usage.");
    }

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
    try {
      new SimpleDateFormat(DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unsupported date format pattern '"
                                         + DshipContext.INSTANCE.get(Constants.DATE_FORMAT_PATTERN)
                                         + "'");
    }

    String sct = DshipContext.INSTANCE.get(Constants.SESSION_CREATE_TIME);
    if (sct == null) {
      throw new IllegalArgumentException(Constants.ERROR_INDICATOR + "create time is null.");
    }
    String cp = DshipContext.INSTANCE.get(Constants.COMPRESS);
    if (cp == null) {
      throw new IllegalArgumentException(Constants.ERROR_INDICATOR + "compress info is null.");
    }
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
    if ("download".equals(type)) {

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

      String exponential = DshipContext.INSTANCE.get(Constants.EXPONENTIAL);
      if (exponential != null && !(exponential.equalsIgnoreCase("true") || exponential.equalsIgnoreCase("false"))) {
        throw new IllegalArgumentException(
            "Invalid parameter :  exponential format in double expected 'true' or 'false', found '"
            + exponential
            + "'\nType 'tunnel help " + type + "' for usage.");
      }
    }

    if ("upload".equals(type)) {
      String scan = DshipContext.INSTANCE.get(Constants.SCAN);
      String dbr = DshipContext.INSTANCE.get(Constants.DISCARD_BAD_RECORDS);
      if (scan == null || !(scan.equals("true") || scan.equals("false") || scan.equals("only"))) {
        throw new IllegalArgumentException("-scan, expected:(true|false|only), actual: '" + scan
                                           + "'\nType 'tunnel help " + type + "' for usage.");
      }
      if (dbr == null || !(dbr.equals("true") || dbr.equals("false"))) {
        throw new IllegalArgumentException(
            "Invalid parameter : discard bad records expected 'true' or 'false', found '" + dbr
            + "'\nType 'tunnel help " + type + "' for usage.");
      }

      String bs = DshipContext.INSTANCE.get(Constants.BLOCK_SIZE);
      String mbr = DshipContext.INSTANCE.get(Constants.MAX_BAD_RECORDS);
      if (bs != null) {
        try {
          Long.valueOf(bs);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Illegal number 'block-size', please check config file.");
        }
      }
      if (mbr != null) {
        try {
          Long.valueOf(mbr);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              "Illegal number 'max-bad-records', please check config file.");
        }
      }

      String path = DshipContext.INSTANCE.get(Constants.RESUME_PATH);
      File sFile = new File(path);
      if (!sFile.exists()) {
        throw new IllegalArgumentException("upload File not found: '" + path
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
    setContextValue(Constants.DATE_FORMAT_PATTERN, Constants.DEFAULT_DATE_FORMAT_PATTERN);
    setContextValue(Constants.NULL_INDICATOR, Constants.DEFAULT_NULL_INDICATOR);
    setContextValue(Constants.SCAN, Constants.DEFAULT_SCAN);
    setContextValue(Constants.HEADER, Constants.DEFAULT_HEADER);

    setContextValue(Constants.RESUME_BLOCK_ID, "1");
    setContextValue(Constants.SESSION_CREATE_TIME,
                    String.valueOf(System.currentTimeMillis()));
    setContextValue(Constants.COMPRESS, "true");
    setContextValue(Constants.THREADS, String.valueOf(Constants.DEFAULT_THREADS));
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
      delimiter =  delimiter.replaceAll("\\\\r", "\r").replaceAll("\\\\n", "\n")
          .replaceAll("\\\\t", "\t");

      Matcher matcher = UNICODE_PATTERN.matcher(delimiter);
      if (matcher.matches()) {
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


  private static void processArgs(String[] remains, boolean isUpload)
      throws IOException {

    String path = null;
    String partition = null;
    String table = null;

    if (remains.length == 3) {
      path = isUpload ? remains[1] : remains[2];
      String desc = isUpload ? remains[2] : remains[1];

      String[] ds = desc.split("/");
      String[] tp = ds[0].split("\\.");
      if (tp.length == 1) {
        table = tp[0];
      } else if (tp.length == 2) {
        table = tp[1];
        setContextValue(Constants.TABLE_PROJECT, tp[0]);
      } else {
        throw new IllegalArgumentException("Invalid parameter: project.table \nType 'tunnel help "
                                           + (isUpload ? "upload" : "download") + "' for usage.");
      }

      if (ds.length == 2) {
        partition = new PartitionSpec(ds[1]).toString();
      } else if (ds.length > 2) {
        throw new IllegalArgumentException(
            "Invalid parameter: project.table/partition\nType 'tunnel help "
            + (isUpload ? "upload" : "download") + "' for usage.");
      }
    } else {
      throw new IllegalArgumentException("Unrecognized command\nType 'tunnel help "
                                         + (isUpload ? "upload" : "download") + "' for usage.");
    }

    setContextValue(Constants.TABLE, table);
    setContextValue(Constants.PARTITION_SPEC, partition);
    setContextValue(Constants.RESUME_PATH, FileUtil.expandUserHomeInPath(path));
  }


  private static void loadConfig()
      throws IOException, ODPSConsoleException {

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
                           + Util.toHumanReadableString(Constants.DEFAULT_RECORD_DELIMITER ))
                       .hasArg().withArgName("ARG").create("rd"));
    opts.addOption(OptionBuilder.withLongOpt(Constants.DATE_FORMAT_PATTERN)
                       .withDescription("specify date format pattern, default "
                                        + Constants.DEFAULT_DATE_FORMAT_PATTERN)
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
                       .hasArg().withArgName("ARG").create());

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
    return opts;
  }

  public static Options getDownloadOptions() {
    Options opts = getGlobalOptions();
    opts.addOption(OptionBuilder.withLongOpt(Constants.LIMIT)
                       .withDescription("specify the number of records to download")
                       .hasArg().withArgName("ARG").create());
    opts.addOption(OptionBuilder.withLongOpt(Constants.EXPONENTIAL)
                       .withDescription("When download double values, use exponential express if necessary. Otherwise at most 20 digits will be reserved. Default false")
                       .hasArg().withArgName("ARG").create("e"));
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
