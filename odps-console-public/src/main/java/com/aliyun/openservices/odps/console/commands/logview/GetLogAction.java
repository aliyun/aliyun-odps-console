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

package com.aliyun.openservices.odps.console.commands.logview;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.LogType;
import com.aliyun.odps.OdpsException;
import com.aliyun.openservices.odps.console.ODPSConsoleException;

public class GetLogAction extends LogViewBaseAction {

  public static final String ACTION_NAME = "get";

  private String fid; // friendly id
  private LogType logType = LogType.STDOUT;
  private Integer size = new Integer(8 * 1024);

  @Override
  public void parse(String[] args) throws LogViewArgumentException {
    if (args.length < 1) {
      throw new LogViewArgumentException("A log entry must be specified");
    }
    fid = args[0];
    Options options = getOptions();
    CommandLineParser parser = new GnuParser();
    try {
      if (args.length > 1) {
        CommandLine cl = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
        configure(cl);
      }
    } catch (ParseException e) {
      throw new LogViewArgumentException(e.getMessage());
    }
  }

  @Override
  public void configure(CommandLine cl) throws LogViewArgumentException {
    super.configure(cl);
    if (cl.hasOption("T")) {
      String logTypeStr = cl.getOptionValue("T").toLowerCase();
      logType = LogType.fromString(logTypeStr);
    }
    if (cl.hasOption('s')) {
      try {
        size = Integer.valueOf(cl.getOptionValue('s'));
      } catch (Throwable e) {
        throw new LogViewArgumentException("Invalid format of -s option, "
            + "which must be an number.");
      }
      if (size <= 0 || size > 1024 * 1024) {
        throw new LogViewArgumentException("Invalid size limit " + size
            + ", which must be larger than 0 and less or equal than 1MB");
      }
    }
  }

  @Override
  public void run() throws OdpsException, ODPSConsoleException {
    if (ctx.getInstance() == null) {
      throw new LogViewArgumentException("No instance specified, please list at first");
    }
    String logId = ctx.getLogDir().get(fid);
    if (logId == null) {
      throw new ODPSConsoleException("Log entry '" + fid + "' of instance '"
          + ctx.getInstance().getId() + "' did not found");
    }
    String logContent = ctx.getInstance().getLog(logId, logType, size);
    BufferedReader bReader = new BufferedReader(new StringReader(logContent));
    String line;
    try {
      while ((line = bReader.readLine()) != null) {
        // Filtering SetEnv lines
        if (!line.startsWith("SetEnv")) {
          getWriter().writeResult(line);
        }
      }
    } catch (IOException e) {
      throw new OdpsException(e);
    }
  }

  @Override
  public String getActionName() {
    return ACTION_NAME;
  }

  @SuppressWarnings("static-access")
  protected Options getOptions() {
    Options options = super.getOptions();
    options.addOption(OptionBuilder.withDescription("stdout or stderr").withArgName("log type")
        .hasArg().withLongOpt("type").create("T"));
    options.addOption(OptionBuilder
        .withDescription("print last <size> bytes of log, default is 8KB").withArgName("size")
        .hasArg().create('s'));
    return options;
  }

  @Override
  protected String getHelpPrefix() {
    return "log " + getActionName() + " <process id>";
  }

}
