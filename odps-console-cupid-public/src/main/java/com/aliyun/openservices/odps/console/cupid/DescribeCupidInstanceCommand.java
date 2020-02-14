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

package com.aliyun.openservices.odps.console.cupid;

import static com.aliyun.odps.cupid.requestcupid.ApplicationMetaUtil.getCupidInstanceMeta;

import java.io.PrintStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.openservices.odps.console.ExecutionContext;
import com.aliyun.openservices.odps.console.ODPSConsoleException;
import com.aliyun.openservices.odps.console.commands.AbstractCommand;
import com.aliyun.openservices.odps.console.cupid.common.CupidSessionConf;

import apsara.odps.cupid.protocol.CupidTaskParamProtos;

public class DescribeCupidInstanceCommand extends AbstractCommand {

  private String instanceID;

  public static final String[] HELP_TAGS = new String[]{"cupid", "desc", "describe"};

  public static void printUsage(PrintStream stream) {
    stream.println("Usage: describe|desc cupid instance -i <instanceID>");
    stream.println("       Get details information of a cupid instance like a kubernetes cluster " +
                   "or a yarn cluster whose ID is <instanceID>");
    stream.println();
  }

  private DescribeCupidInstanceCommand(String commandText, ExecutionContext context,
                                       String[] args) throws ODPSConsoleException {
    super(commandText, context);

    try {
      parseArgs(args);
    } catch (ParseException e) {
      printUsage(System.err);
      throw new ODPSConsoleException(e.getMessage(), e);
    }
  }

  public static DescribeCupidInstanceCommand parse(String cmd, ExecutionContext cxt)
      throws ODPSConsoleException {

    String regstr = "\\s*(describe|desc)\\s+(?:cupid)\\s+instance([\\s\\S]*)";
    Pattern pattern = Pattern.compile(regstr, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(cmd);

    if (matcher.matches()) {
      String args = matcher.group(2).replaceAll("\\s+", " ").trim();
      return new DescribeCupidInstanceCommand(cmd, cxt, args.split(" "));
    }

    return null;
  }

  /**
   * Init describe cmd options for cmd parsing, add new options here if there are more.
   *
   * @return Options
   */
  private static Options initDescribeOptions() {
    Options options = new Options();

    Option instanceIDOption = new Option("i", "instanceID", true, "instance ID");
    instanceIDOption.setRequired(true);
    options.addOption(instanceIDOption);

    return options;
  }

  /**
   * Parse arguments and assign to members
   *
   * @param args
   *     arguments as a String array
   * @throws ParseException
   *     if parse failed
   */
  private void parseArgs(String[] args) throws ParseException, ODPSConsoleException {
    Options options;

    options = initDescribeOptions();

    CommandLineParser parser = new DefaultParser();

    CommandLine cmd = parser.parse(options, args);

    this.instanceID = cmd.getOptionValue("instanceID");

  }

  @Override
  public void run() throws ODPSConsoleException {
    describeCupidInstance();
  }

  private void describeCupidInstance() throws ODPSConsoleException {
    // Start a cupidSession
    CupidConf cupidConf = CupidSessionConf.getBasicCupidConf(this.getContext());
    CupidSession cupidSession = new CupidSession(cupidConf);

    try {
      CupidTaskParamProtos.ApplicationMeta
          applicationMeta = getCupidInstanceMeta(this.instanceID, cupidSession);
      if (applicationMeta != null) {
        getWriter().writeError("");
        getWriter().writeResult(applicationMeta.toString());
      } else {
        throw new ODPSConsoleException("Getting Cupid Instance Meta failed. ");
      }
    } catch (Exception e) {
      throw new ODPSConsoleException("Getting Cupid Instance Meta failed: " + e.getMessage(), e);
    }
  }

  String getInstanceID() {
    return instanceID;
  }

}
